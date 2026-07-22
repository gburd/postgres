# Multithreaded PostgreSQL Implementation Plan

This plan is intentionally ambitious, but it is staged so that each phase
leaves the tree in a coherent state. The first implementation target is native
thread-per-session PostgreSQL for regular client backends. A follow-on
auxiliary-worker phase makes normal threaded server mode stop forking in-tree
server-owned workers. Pooled scheduling comes later.

The ordering is deliberately practical:

1. establish a real backend loop boundary in process mode;
2. attach that boundary to explicit runtime/session/backend objects;
3. classify and isolate enough mutable state for thread-per-session;
4. make blocking waits targetable and wakeable for threaded backends;
5. launch threaded client backends;
6. make in-tree auxiliary worker families threaded so normal threaded server
   mode no longer forks for server-owned workers;
7. make sessions movable only at the top-level frontend protocol boundary;
8. defer more complex scheduler-yielding boundaries until the protocol
   scheduler is real, hardened, and measured.

## Current Branch Baseline

The branch starts from PostgreSQL `REL_19_BETA1`.

Already landed:

- local reference material in `refs/`;
- root-level architecture documentation;
- a root-level agent guide;
- initial process-mode loop extraction:
  - `PgSessionLoopState`;
  - `PgSessionRecoverError()`;
  - `PgSessionStep()`;
  - `PgSessionRun()`;
- runtime/session/backend scaffolding:
  - `PgRuntime`;
  - `PgCarrier`;
  - `PgBackend`;
  - `PgSession`;
  - `PgConnection`;
  - `PgExecution`;
- explicit session step/resume boundary:
  - `PgSessionBootstrap()`;
  - `PgSessionStep(PgSession *, PgStepBudget)`;
  - `PgSessionRun(PgSession *)`;
  - session-owned extended-protocol skip state.

The loop extraction and runtime scaffolding keep process behavior unchanged and
do not expose threaded mode.

## Phase 0: Reference Audit And Invariants

Status: complete for the current stage.

Goal: preserve the relevant prior art and identify the invariants that must not
be broken while the backend process model is split apart.

Tasks:

- Keep `refs/` as the local reference set.
- Extract the useful ideas from Heikki's threading branch:
  - global annotations;
  - logical interrupts;
  - thread launch mechanics;
  - extension module gating;
  - GUC handling experiments;
  - session resource owner.
- Identify high-risk backend invariants before moving code:
  - top-level error recovery;
  - transaction abort cleanup;
  - signal mask assumptions;
  - memory context current-state assumptions;
  - `PGPROC` and lock ownership;
  - fd ownership and virtual fd cache;
  - relcache/catcache invalidation behavior.

Deliverables:

- `AGENTS.md`
- `MULTITHREADED_ARCHITECTURE.md`
- `MULTITHREADED_PLAN.md`

Validation:

- documentation review;
- no code behavior changed.

## Phase 1: Minimal Main Loop Boundary

Status: initial implementation complete.

Goal: split the process-mode `PostgresMain()` loop just enough to create a real
step boundary, while preserving synchronous process-mode behavior.

Completed shape:

- volatile loop locals moved into `PgSessionLoopState` where needed for
  `siglongjmp` safety;
- top-level error recovery extracted into `PgSessionRecoverError()`;
- one command cycle extracted into `PgSessionStep()`;
- process-mode runner added as `PgSessionRun()`;
- `PostgresMain()` delegates to the process-mode runner after initialization.

Current shape after Phase 3:

`PgSessionStep(PgSession *, PgStepBudget)` owns the protected bottom
`sigsetjmp` boundary used by scheduler callers, while
`PgSessionStepUnprotected()` remains private. Current process/thread runners may
install their own persistent top-level boundary and call the private helper
inside that boundary; future scheduler code must use the protected step API
rather than treating `PgSessionRun()` as the scheduler entrypoint.

Validation:

- backend build succeeds;
- core process-mode regression tests pass;
- no threaded mode exposed.

## Phase 2: Runtime And State Scaffolding

Status: complete for the current stage.

Goal: introduce the runtime/session/backend vocabulary and object skeletons,
then connect the existing process-mode startup path to those objects without
changing behavior.

Likely changes:

- Add headers for runtime/session/backend/carrier concepts.
- Add current-context pointers with process-mode initialization.
- Introduce the broader object as `PgSession` and embed or reference the
  existing `Session` object initially.
- Do not rename `Session` or repurpose `CurrentSession` in the first
  scaffolding commit.
- Add `PgRuntime`, `PgCarrier`, `PgBackend`, `PgSession`, `PgConnection`, and
  `PgExecution` as thin objects.
- Move or attach `PgSessionLoopState` to the new object model.
- Add comments documenting ownership boundaries.
- Add assertions that current runtime/backend/session/execution pointers are
  initialized before new object-owned state is accessed.

Expected commit shape:

- one commit for type declarations and no-op process-mode initialization;
- one commit connecting current process-mode startup to the skeleton;
- one commit attaching main-loop state to `PgSession`;
- no broad call-site churn yet.

Validation:

- build succeeds;
- core regression tests pass in process mode;
- no threaded mode exposed.

## Phase 3: Complete Main Loop Unwinding

Status: complete for the current stage.

Goal: finish splitting `PostgresMain()` into stateful pieces while preserving
process-mode behavior and making the protected step contract explicit.

Likely changes:

- Extract top-level session bootstrap from `PostgresMain()`.
- Change the step shape toward:

```c
PgStepResult PgSessionStep(PgSession *session, PgStepBudget budget);
void PgSessionRun(PgSession *session);
```

- Make `PgSessionStep()` the protected public entrypoint, or make it verify
  that the matching session error boundary is active before processing work.
- Keep unprotected helpers private.
- Move remaining loop/session flags into the session/execution state where
  practical:
  - `ignore_till_sync`;
  - `doing_extended_query_message`;
  - debug query string ownership if feasible;
  - statement/protocol metadata that naturally belongs to a command execution.
- Keep early `PgSessionStep()` blocking inside `ReadCommand()` and command
  execution. That is acceptable until scheduler-aware waits exist.

Validation:

- process-mode regression tests pass;
- targeted error recovery tests still behave correctly;
- extended query protocol still handles skip-until-sync correctly;
- cancellation during command read still works;
- no unhandled `ERROR` escapes past the protected step entrypoint.

Exit gate:

- Gate A is part of Phase 3 completion. Before leaving Phase 3, run the Gate A
  checks from the Test Strategy section: core regression, relevant isolation
  tests, and targeted protocol/error-recovery tests.

## Phase 4: Global Lifetime Annotation

Goal: create visibility into mutable global state before moving it, now that
there are concrete runtime/session/backend/execution owners to classify
against.

Scope boundary: Phase 4 establishes the vocabulary, scanner, baseline, and
new-code enforcement. It is not expected to classify every existing mutable
global in the tree. Existing unclassified globals remain in the baseline as
explicit migration debt for later phases.

Likely changes:

- Add lifetime annotation macros inspired by Heikki's branch.
- Add or adapt a static tool to find unclassified mutable globals.
- Start with annotations that do not change generated code.
- Classify globals by ownership:
  - runtime-global;
  - immutable singleton;
  - dynamic singleton;
  - backend-local;
  - session-local;
  - execution-local;
  - carrier-local;
  - connection-local;
  - shared-memory state.

Expected commit shape:

- one commit for annotation macros and tooling;
- several focused commits annotating subsystems;
- report output that is useful enough to guide Phase 8.

Validation:

- process-mode tests pass;
- static tool can run and produce a useful report;
- existing unclassified mutable globals are captured in a checked-in baseline;
- new mutable globals require explicit classification or an explicit baseline
  update.

## Phase 5: Logical Interrupts And Timeouts

Goal: replace process-signal-shaped backend communication with logical
interrupts that work for both processes and threads, and make timeout delivery
target logical backends rather than the whole process.

Likely changes:

- Add a backend interrupt mailbox.
- Convert signal handlers to set logical interrupt bits.
- Route cancellation, termination, config reload, notify catchup, procsignal
  barriers, and timeout events through the interrupt system.
- Split timeout registration from timeout delivery so timeout expiry records a
  target backend/execution and sets logical interrupt bits for that target.
- Preserve process-mode `SIGALRM` behavior as an implementation detail where
  useful, but do not make thread mode depend on per-backend Unix alarm signals.
- Keep `CHECK_FOR_INTERRUPTS()` as the common service point.
- Preserve existing signal delivery in process mode as an external transport.

Expected commit shape:

- one commit introducing interrupt types and mailboxes;
- focused commits replacing families of signal/procsignal uses;
- targeted compatibility wrappers where a full conversion would be too broad.

Validation:

- cancellation tests;
- termination interrupt delivery tests that preserve current process-mode exit
  behavior;
- config reload tests;
- LISTEN/NOTIFY behavior;
- statement, lock, transaction, idle-in-transaction, and idle-session timeouts;
- hot standby recovery conflict behavior where practical;
- process-mode regressions.

Phase 5 completion note:

- The implementation routes recovery-conflict interrupts through the logical
  backend mailbox and preserves the existing process-mode behavior.
- A dedicated hot-standby recovery-conflict fixture was not built during Phase
  5. Phase 5 was considered complete after tracing the existing
  recovery-conflict delivery path and confirming it now passes through the
  logical interrupt machinery. Treat the missing fixture as a validation gap to
  cover in Gate B or a focused follow-up, not as an incomplete Phase 5
  implementation item.
- This is a deliberate conclusion from working through the phase. The existing
  recovery-conflict path already reaches `CHECK_FOR_INTERRUPTS()` via backend
  interrupt state; Phase 5 changed that backend-visible state to use the
  logical interrupt machinery. A new standby-cluster fixture would add direct
  regression coverage, but it is not required to claim the implementation work
  for Phase 5 complete.
- In other words, this was a deliberate validation deferral after inspection,
  not an indication that Phase 5 still needs implementation work before Phase
  6 can proceed.
- See `MULTITHREADED_PHASE5_INTERRUPTS.md` for the phase-specific note.

## Phase 6: Backend Lifecycle And Exit

Goal: make backend termination logical so a threaded backend can exit without
terminating or corrupting the whole runtime.

Likely changes:

- Identify direct and indirect callers of `proc_exit`, `exit`, and fatal exit
  helpers in backend code.
- Split logical backend exit from process exit.
- Route `on_proc_exit`, `before_shmem_exit`, and `shmem_exit` callbacks through
  backend/runtime-aware cleanup.
- Ensure one logical backend can release resources and detach from shared state
  while other threaded backends continue.
- Preserve current process-mode behavior.
- Define which paths still escalate to runtime/process termination, especially
  `PANIC` and postmaster death.

Validation:

- normal client disconnect cleanup;
- `FATAL` during active transaction;
- repeated connect/disconnect stress in process mode;
- temporary file cleanup;
- DSM/DSA detach cleanup;
- callback ordering remains compatible in process mode.

Implementation notes:

- [MULTITHREADED_PHASE6_EXIT.md](MULTITHREADED_PHASE6_EXIT.md) records the
  current logical backend exit boundary, migrated call-site families, remaining
  process/runtime exit ownership decisions, validation already run, and the
  deferred thread-runtime proof that belongs to Phase 10.

Phase 6 completion note:

- The first real thread-per-session runtime proof is intentionally deferred to
  Phase 10, where threaded backend launch exists. Phase 6 is complete when the
  backend-exit lifecycle split, backend-local cleanup ownership, process-mode
  compatibility, and post-cleanup runtime handoff contract are implemented and
  validated.

Exit gate:

- Gate B is part of Phase 6 completion. Before leaving Phase 6, run the Gate B
  checks from the Test Strategy section: `check-world` or a documented
  near-equivalent, plus focused cancellation, timeout, config reload,
  LISTEN/NOTIFY, and disconnect/FATAL tests.

## Phase 7: Extension Backend Model Gate

Goal: prevent unsafe extension loading in threaded mode and establish the route
for in-tree extensions.

Likely changes:

- Extend `Pg_magic_struct` with backend model metadata.
- Make default `PG_MODULE_MAGIC` process-only.
- Add explicit opt-in macros for threaded compatibility.
- Teach `dfmgr.c` to reject incompatible modules when threaded mode is active.
- Add a test-only runtime/backend-model override so loader policy can be tested
  before threaded backend launch exists.
- Audit PL/pgSQL first.
- Define the minimum in-tree module allowlist for threaded regression tests,
  including PL/pgSQL, required encoding conversion modules, regression-test
  helper modules, and any module loaded automatically by the selected tests.
- Add per-session extension state APIs if needed by PL/pgSQL or bundled
  modules.

Suggested backend model levels:

- process only;
- thread-per-session safe;
- pooled-scheduler/task reentrant.

Validation:

- incompatible test extension is rejected under the test-only threaded backend
  model;
- existing process mode loads extensions as before;
- metadata parsing and version compatibility are covered;
- changing the active extension backend model is rejected when any already
  loaded module is incompatible with the requested model;
- PL/pgSQL audit has a concrete migration path, recorded in
  `MULTITHREADED_PHASE7_EXTENSIONS.md`;
- real threaded-mode PL/pgSQL and allowlist validation are deferred to the
  thread-per-session runtime gate.

## Phase 8: Thread-Safety Floor

Goal: make enough backend-local state private to each logical backend that
thread-per-session launch is not sharing unsafe plain globals.

Acceptance boundary: Phase 8 is the first hard global-state checkpoint. The
Phase 4 baseline may still contain unrelated unclassified globals after this
phase, but every global in the required floor below must either be classified
with the correct lifetime, moved behind an owned object, made thread-local as a
temporary bridge, or proven to be immutable/shared-memory-safe.

Required floor:

- current memory context state;
- current resource owner state;
- `MyProc` and `PGPROC` ownership state;
- `MyProcPort` and frontend protocol buffers;
- interrupt pending flags and interrupt holdoff counters;
- timeout pending flags and timeout registration state;
- GUC backing variables and GUC nesting state;
- error context and exception stack state;
- current transaction/session identity globals;
- temporary file and virtual fd owner state.

Likely changes:

- Use thread-local compatibility state where that preserves current
  process-per-session semantics.
- Prefer object-owned state where the ownership boundary is already clear.
- Add assertions that the current runtime/backend/session/execution pointers
  are initialized before backend-local state is accessed.
- Keep process-mode behavior unchanged.

Validation:

- process-mode full regression tests;
- static global report shows that no item in the required floor remains as an
  unsafe unclassified plain mutable process global;
- any remaining unclassified globals are outside the required floor and remain
  tracked as explicit migration debt;
- targeted tests for memory context, resource owner, GUC, interrupt, timeout,
  protocol, and fd cleanup behavior.

Exit gate:

- Gate C is part of Phase 8 completion. Before leaving Phase 8, run the Gate C
  checks from the Test Strategy section: `check-world`, static global report
  checks, extension load tests under the test-only threaded backend model, and
  PL/pgSQL process-mode regression tests. The gate fails if any Phase 8
  required-floor global remains unsafe and unclassified.

## Phase 9: Thread-Compatible Wait/Wakeup Boundary

Status: complete for the thread-per-session prerequisite. See
`MULTITHREADED_PHASE9_WAIT_BOUNDARY.md` for the wait-family inventory,
target-backend wake path, and validation record.

Goal: make long waits visible, targetable, and wakeable before threaded backend
launch. This is not the pooled scheduler yet; waits may still block the current
OS thread in process mode and thread-per-session mode.

Likely changes:

- Inventory unbounded waits that can hide a backend from cancellation or
  termination:
  - frontend command reads;
  - frontend output flushes;
  - latch and wait-event-set waits;
  - lock waits;
  - condition variable waits;
  - timeout waits.
- Introduce `PgWaitSpec` and `PgSuspend()` or equivalent as the common visible
  wait boundary.
- Record the current waiting backend/session/execution before entering a long
  wait.
- Connect logical interrupts to a wake mechanism for the target backend, such as
  latch wakeups or a platform-specific thread wake primitive.
- Preserve blocking behavior for process mode and thread-per-session mode.
- Do not introduce runnable queues or pooled carrier scheduling in this phase.

Important rule:

Before regular backends can run as threads, an idle or blocked backend must be
wakeable for cancellation, termination, and timeout delivery without depending
on process-directed Unix signals.

Validation:

- process-mode tests pass;
- cancellation while blocked still works;
- idle and active termination paths wake blocked backends;
- idle timeout and transaction timeout behavior remains correct;
- no lost wakeups in common wait paths.

## Phase 10: Thread-Per-Session Runtime

Status: complete for the first thread-per-session target. See
`MULTITHREADED_PHASE10_THREAD_RUNTIME.md` for the launch, cleanup, worker
handoff, and Gate D validation record.

Goal: run regular client backends as OS threads inside one server runtime.

Likely changes:

- Add PostgreSQL thread portability layer if not already present.
- Add `multithreaded` or equivalent experimental GUC.
- Add backend launch path that can choose process or thread.
- Initialize carrier-local state for each thread.
- Initialize current runtime/backend/session/execution pointers.
- Ensure signal masks and handlers are not incorrectly installed per thread.
- Use the Phase 9 wait/wakeup boundary for blocked backend cancellation,
  termination, and timeout delivery.
- Preserve process-mode launch path.

Conservative scope:

- regular client backends first;
- startup-time process workers may still exist until Phase 11, but regular
  threaded server mode must not launch new fork-without-exec server-owned
  subprocesses after backend thread carriers have started;
- auxiliary worker families that need late launch, including autovacuum
  workers, must be disabled, gated off, or routed to a process-safe path until
  Phase 11 gives them thread carriers;
- Phase 10 suppressed process-backed parallel workers in threaded sessions
  until the worker runtime existed, with callers falling back to leader-only
  execution where PostgreSQL already supports that. Phase 11 supersedes that
  temporary restriction with thread-backed core parallel workers;
- third-party background workers can be gated off or process-only until the
  worker runtime and extension metadata are audited;
- unsafe extensions rejected through backend model metadata.

Validation:

- multiple concurrent SQL sessions in threaded mode;
- cancellation and termination of one threaded backend, including while blocked;
- connection startup and teardown;
- transaction abort and error recovery;
- basic isolation tests;
- PL/pgSQL smoke and regression tests in threaded mode;
- incompatible extensions rejected in threaded mode;
- process-mode full test suite.

Exit gate:

- Gate D is part of Phase 10 completion. Before leaving Phase 10, run the Gate
  D checks from the Test Strategy section: full process-mode tests plus the
  threaded smoke/regression subset for concurrent clients, cancellation,
  termination, `ERROR` recovery, transaction abort cleanup, PL/pgSQL,
  incompatible extension rejection, and repeated connect/disconnect stress.
  The in-tree `test_backend_runtime` TAP smoke is part of the Phase 10
  regression surface and should cover the compact concurrent-client,
  cancel/terminate, SQL `ERROR`, PL/pgSQL, incompatible module rejection, and
  abandoned-client cleanup smoke, plus transaction-abort cleanup and repeated
  connect/disconnect coverage. It is not a substitute for the broader
  killed-client stress or full process-mode test suite.
  Verify that normal threaded server mode does not fork late server-owned
  worker subprocesses after backend thread carriers exist. Until Phase 11,
  document any worker family that is explicitly deferred or disabled in
  threaded mode.

## Phase 11: Auxiliary Worker Thread Runtime

Status: complete for the current thread-per-session worker-runtime stage. See
`MULTITHREADED_PHASE11_WORKERS.md` for the completed autovacuum
launcher/worker, AIO worker startup handoff and late launch, generic
background-worker compatibility and explicit backend-model metadata, WAL
receiver, WAL summarizer, WAL writer, archiver, checkpointer/background
writer handoff, syslogger handoff, slot sync worker, and logical replication
launcher slices, plus logical replication apply/table-sync, sequence-sync,
and parallel apply slices, core parallel worker thread carriers, online
data-checksum launcher/workers, the remaining audited in-tree server-owned
worker families, and Gate E validation.

Goal: make normal threaded server mode fully threaded for in-tree
server-owned worker families, so the runtime does not fork subprocesses for
ordinary server operation.

This phase is distinct from Phase 10. Phase 10 proves the user-session backend
runtime. Phase 11 proves the worker runtime needed for a server that is
threaded in normal operation.

Explicit non-goals:

- single-user mode;
- bootstrap mode;
- frontend command-line utilities;
- postmaster/control-plane process lifetime;
- crash-escalation paths where terminating the process or whole runtime is the
  correct behavior.

### Threaded crash policy and restart_after_crash (2026-07-11)

A genuine backend crash (memory-corrupting SIGSEGV/SIGBUS/etc.) under the
threaded runtime is FAIL-STOP for the whole process, by necessity and design:

- The postmaster is a thread in the SAME address space as the carriers (unlike
  forked mode, where the postmaster is a separate, uncorrupted process).  A
  fiber that corrupts memory can therefore corrupt the postmaster too, so the
  postmaster CANNOT survive to run in-process crash recovery.  In-process
  `restart_after_crash` (the forked HandleChildCrash -> re-init cycle) is thus
  architecturally impossible after a genuine threaded crash and is intentionally
  NOT wired for threaded backends -- this is the "crash-escalation" bullet of
  the process-lifetime exceptions above.
- Two crash sub-paths both end in whole-process exit: (a) a fiber-spawned
  backend/worker fault the libxtc guard can contain -> DOWN ->
  g_xtc_genuine_crash -> ExitPostmaster(1); (b) a pooled-affine session running
  inline on a plain carrier thread (not a libxtc coro) cannot be contained ->
  the guard restores SIG_DFL and re-raises -> the process dies by signal.
- The runtime drops RLIMIT_CORE to 0 at carrier start (commit 43c4ac752e8) so
  the re-raised fault terminates instantly and closes all client sockets at
  once, instead of hanging the crashing client for tens of seconds while the
  kernel writes a multi-thread multi-GB core.  PG_XTC_ALLOW_CORE=1 keeps cores
  for active debugging.
- Restart is therefore an EXTERNAL-SUPERVISION responsibility: death-by-signal
  (exit 128+N) is exactly what systemd `Restart=on-failure`/`on-abnormal`, a
  process manager, or an operator restart loop acts on.  A future in-tree
  option is a separate lightweight watchdog/control process (candidate:
  libxtc's cross-fork xtc_xproc from v1.12.0) that outlives a carrier-process
  crash and re-execs the server; this stays behind the postmaster/control-plane
  process-lifetime exception until then.  TAP 010 pins the contract: crash ->
  fast fail-stop -> committed data survives an external restart with clean
  recovery.

Likely changes:

- Add an explicit worker runtime owner, such as `PgWorker` or
  `PgAuxiliaryWorker`, rather than folding every worker into `PgSession`.
- Reuse `PgRuntime`, `PgCarrier`, logical interrupt, wait/wakeup, and backend
  exit machinery where the worker participates in normal server scheduling.
- Extend postmaster/launch-backend supervision so in-tree worker families can
  choose process carriers or thread carriers.
- Convert in-tree auxiliary worker families to thread carriers in threaded
  mode:
  - autovacuum launcher and workers have initial thread-carrier slices;
  - checkpointer/background writer handoff, WAL writer, and archiver have
    initial thread-carrier slices;
  - syslogger startup is process-backed until `PM_RUN`, then handed off to a
    thread carrier;
  - startup/recovery has an initial thread-carrier slice in threaded mode;
  - WAL receiver, WAL summarizer, and slot sync worker have initial
    thread-carrier slices;
  - startup-time AIO method workers are handed off after `PM_RUN`, and late
    AIO method workers have an initial thread-carrier slice;
  - logical replication launcher, apply workers, table-sync workers,
    sequence-sync workers, and parallel apply workers have initial
    thread-carrier slices through explicit background-worker backend-model
    metadata;
  - core parallel query, parallel index build, and parallel vacuum workers
    have an initial thread-carrier slice through explicit background-worker
    backend-model metadata;
  - online data-checksum launcher and per-database workers have an initial
    thread-carrier slice through explicit background-worker backend-model
    metadata;
  - the in-core `REPACK (CONCURRENTLY)` decoding worker has an initial
    thread-carrier slice through explicit background-worker backend-model
    metadata;
  - the bundled `pg_prewarm` autoprewarm leader and per-database workers have
    an initial thread-carrier slice through explicit background-worker
    backend-model metadata, and the autoprewarm shared-state attachment
    pointer now lives in `PgBackend.extension_modules` rather than
    contrib-local TLS;
  - the bundled `auto_explain` custom-GUC backing variables now live in
    `PgSession.extension_modules`, and its executor nesting/sampling state now
    lives in `PgExecution.extension`, leaving only runtime-global hook-chain
    pointers as module-local static state;
  - the bundled `pg_stash_advice` persistence worker has an initial
    thread-carrier slice through explicit background-worker backend-model
    metadata, with its `pg_plan_advice` dependency marked for the same
    backend model. `pg_plan_advice` session-local custom-GUC backing state and
    advice-generation request state now live in `PgSession.extension_modules`
    rather than contrib-local TLS globals. `pg_stash_advice` stash-name GUC
    state now lives in `PgSession.extension_modules`, and its backend-local
    DSM/DSA/dshash attachment pointers live in `PgBackend.extension_modules`.
- Require generic background workers to declare
  `BgWorkerBackendThreadPerSession` before they can run on thread carriers.
  The zero/default registration value remains process-only, so existing
  third-party workers are rejected in threaded mode when a thread carrier is
  required.
- The initial in-tree generic background-worker audit is complete: all current
  `RegisterBackgroundWorker()` and `RegisterDynamicBackgroundWorker()` call
  sites under `src/`, `contrib/`, and `src/test/modules` are either opted into
  `BgWorkerBackendThreadPerSession` or intentionally left process-only as
  negative compatibility coverage.
- Define worker exit semantics separately from user-session exit semantics:
  normal worker exit must clean up one worker, while `PANIC`, postmaster death,
  and unrecoverable runtime corruption still terminate the process or runtime.

Validation:

- threaded normal-mode server start/stop without forked in-tree
  server-owned worker subprocesses after runtime startup;
- autovacuum launcher and worker smoke tests in threaded mode;
- checkpointer, background writer, WAL writer, archiver, and syslogger smoke
  tests in threaded mode;
- WAL receiver, WAL summarizer, slot sync worker, logical replication
  launcher, and logical replication worker smoke tests where local test
  infrastructure supports them;
- startup/recovery, physical basebackup, hot-standby replay, and promotion
  smoke tests in threaded mode;
- parallel query, parallel index build, and parallel vacuum worker smoke tests
  where local test infrastructure supports them;
- AIO worker smoke tests;
- cancellation, shutdown, restart, and failure escalation for threaded
  workers;
- process-mode worker behavior remains unchanged;
- third-party background workers are rejected or kept process-only unless
  explicitly marked thread-worker safe through background-worker backend-model
  metadata.

Exit gate:

- Gate E is part of Phase 11 completion. Before leaving Phase 11, run the Gate
  E checks from the Test Strategy section: threaded worker smoke tests for all
  in-tree server-owned worker families, worker cancellation/shutdown/restart
  and failure escalation tests, documented process-lifetime exception checks,
  full process-mode tests, and the threaded-mode worker subset.

## Phase 12: State Migration From TLS To Objects

Status: closed for the scoped Gate E2-Core target. Phase 13 may start from this
state once the final validation baseline remains green. If a Phase 12 guard
later fails, reopen only the evidence-driven blocker rather than resuming broad
state-migration churn.

Goal achieved: core backend/session/connection/execution/carrier state has
explicit runtime-object ownership sufficient for thread-per-session startup,
normal SQL, PL/pgSQL, core GUC behavior, worker handoff, teardown, and reconnect
coverage. Process mode remains supported.

`MULTITHREADED_PHASE12_STATE.md` is the chronological implementation ledger and
validation/audit trail for this phase. It is intentionally verbose and should
not be treated as the active plan for Phase 13. The latest closeout audit lives
at the end of that file; this section is the concise source of truth for what
Phase 12 delivered and what it deferred.

Completed work:

- Introduced the core runtime roots: `PgProcess`, `PgThread`, `PgSession`,
  `PgBackend`, `PgConnection`, and `PgExecution`.
- Moved the core backend runtime bridge toward owner-adjacent subsystem files,
  leaving `src/backend/utils/init/backend_runtime.c` focused on root runtime
  construction, current-pointer installation, process/thread symmetry, and
  top-level lifecycle orchestration.
- Split backend-runtime C tests into object-family test files under
  `src/test/modules/test_backend_runtime` instead of growing the original test
  monolith.
- Added checked lifecycle/global guardrails through
  `MULTITHREADED_RUNTIME_LIFECYCLE.tsv`, `MULTITHREADED_RUNTIME_OWNERS.tsv`,
  `gmake check-runtime-lifecycles`, and `gmake check-global-lifetimes`.
- Migrated the core direct-pointer GUC surface needed by the threaded core
  runtime, with process-mode adoption/rebinding preserved.
- Proved the PMChild/thread synchronization contract for current backend and
  worker paths.
- Narrowed startup serialization so ordinary threaded startup no longer depends
  on a broad process-wide startup lock.
- Strengthened threaded teardown evidence with disconnect, abandoned client,
  SQL ERROR recovery, cancel, terminate, FATAL, reconnect, worker handoff,
  mixed/reaping stress, and retained-root accounting checks.
- Added the broader `gmake check-threaded-world-core` validation target for the
  Phase 12 core scope.

Gate E2-Core validation baseline:

- `gmake check`
- `gmake check-threaded`
- `gmake check-threaded-workers`
- `gmake check-threaded-world-core`
- `gmake check-runtime-lifecycles`
- `gmake check-global-lifetimes`
- `git diff --check`

Gate E2-Core exit decision:

- No current runtime-evidenced Gate E2-Core blocker remains open in the latest
  audit.
- Do not continue Phase 12 refactor work as a primary task unless one of the
  validation targets above exposes a concrete regression.
- Future fixes should be targeted from runtime evidence: TAP failures, retained
  root warnings, lifecycle/global-lifetime checker failures, crashes, hangs, or
  process-mode regressions.

Deferred with invariant:

- Contrib-wide threaded support is deferred to Phase 16 / Gate E2-Extensions.
  It is safe for Gate E2-Core because process-only extension entry points are
  rejected in threaded mode and the core TAP suite checks those rejections.
- Bundled procedural languages beyond PL/pgSQL are deferred to Phase 16 / Gate
  E2-Extensions. PL/pgSQL remains in the core validation target; other bundled
  languages must not be required for Phase 13 wait-observability work or
  Phase 14/15 protocol-scheduler work.
- The full custom/extension GUC matrix is deferred to Phase 16 / Gate
  E2-Extensions. The invariant is that core built-in GUC behavior is validated
  by threaded regression/TAP coverage, while ambiguous hook/custom/extension
  paths stay behind the temporary guarded process-wide adoption path until the
  extension gate owns them.
- Broad `src/bin`, interface, and non-core TAP threaded-mode completeness is
  deferred beyond Gate E2-Core. Phase 12 guards catch core backend lifecycle,
  GUC, teardown, and worker regressions; wider client/tool coverage belongs to
  later integration gates.
- Pooled carrier scheduling, fair yielding, and scheduler-visible wait
  semantics are not Phase 12 work. Phase 13 owns wait observability while
  preserving the thread-per-session fallback. Phases 14 and 15 own
  protocol-boundary scheduling only.

## Phase 13: Wait Observability Boundary

Goal: make important backend waits visible, cancellable, and wakeable without
claiming that they release carriers.

Detailed working plan: `MULTITHREADED_PHASE13_PLAN.md`.

Scheduler design constraint: Phase 13 wait-completion records are evidence and
diagnostic plumbing. They are not, by themselves, scheduler-yielding
continuations. The protocol-boundary scheduler design in
`MULTITHREADED_PROTOCOL_SCHEDULER_DESIGN.md` supersedes any older implication
that arbitrary `PgSuspend()` waits should detach a logical backend.

Likely changes:

- Publish wait-completion records for representative blocking wait families.
- Keep `PgSuspend()` as an observable-wait API that may still block the current
  carrier.
- Preserve process-mode behavior and the thread-per-session blocking fallback.
- Make frontend input/output, latch, lock, condition variable, semaphore, and
  timeout waits scheduler-visible for diagnostics and cancellation.
- Keep the Phase 12 interrupt/latch boundary intact:
  logical backend events use `SendInterrupt()`/`RaiseInterrupt()` or mapped
  proc-signal reasons, wait readiness remains latch/CV/wait-event driven until
  represented by a Phase 13 wait-completion record, and process lifecycle
  signalling remains process-shaped.
- Do not install generic scheduler requeue hooks on every wait-completion
  record.

Validation:

- process-mode tests pass;
- thread-per-session tests pass;
- cancellation while blocked still works;
- idle timeout and transaction timeout behavior remains correct;
- no lost wakeups in common wait paths;
- negative coverage proves deep waits publish observability but do not detach a
  logical backend.

## Phase 14: Protocol-Boundary Scheduler Foundation

Goal: introduce the only Phase 14 scheduler-yielding boundary: top-level
frontend protocol input before any new message byte has been consumed.

Design reference: `MULTITHREADED_PROTOCOL_SCHEDULER_DESIGN.md`.

Likely changes:

- Start from a clean Phase 13 wait-observability baseline, or hard-reset and
  cherry-pick only the current work that matches the protocol-boundary design.
- Add a nonblocking frontend message type-byte probe with explicit no-byte,
  byte-available, and EOF/error semantics.
- Add transport wait mask/generation support for the protocol probe, including
  SSL/GSS read/write/buffered-input behavior, or explicitly keep SSL/GSS
  connections carrier-pinned for Phase 14.
- Add explicit protocol-park prepare/commit APIs separate from `PgSuspend()`.
- Teach `PgSessionStep()` to return a prepared protocol-park result, and extend
  `PgStepResult` with normal and fatal logical backend exit outcomes before
  scheduler dispatch depends on it.
- Add parked wake reason and generation/sequence tracking.
- Add a deferred-notify generation/reason marker so idle-in-transaction
  listeners do not spin on unserviceable notifications.
- Add scheduler runnable and parked-protocol queues.
- Add frontend transport-readiness dispatch for parked protocol reads.
- Wake parked sessions on frontend input, disconnect, cancel/die,
  config/catchup/proc-signal work, timeout expiry, postmaster death, and
  scheduler shutdown.
- Add backend-indexed timeout snapshot/wake plumbing, or reattach before
  inspecting/firing current-backend timeout state.
- Add timeout generation validation so stale parked timeout snapshots cannot
  fire after timer reconfiguration or frontend input readiness.
- Add `LISTEN`/`NOTIFY` wake behavior for parked sessions, including the
  `idle in transaction` no-spin rule.
- Add attach/detach assertions for current pointers, TLS mirrors, `PGPROC`,
  latches, `FeBeWaitSet`, memory contexts, resource owners, timeouts, and
  scheduler state.
- Decide the Phase 14 wake object policy for `PGPROC`, backend latch,
  `MyLatch`, and `FeBeWaitSet`; this is an acceptance criterion, not a later
  open question.
- Define the concrete parked wake routing table for frontend transport,
  `SendInterrupt()`, proc-signal fallback, `PGPROC->procLatch`, timeout expiry,
  and postmaster death before adding scheduler queues.

Initial limitations are required, not merely acceptable:

- deep `PgSuspend()` waits remain carrier-pinned;
- frontend output backpressure remains carrier-pinned;
- active command execution remains carrier-pinned;
- extension or subsystem state that is not session-migratable must keep the
  session hard-affine, process-only, or rejected from pooled protocol mode;
- staging implementations may still launch one carrier per client, but must
  not be described as complete pooled-carrier scheduling.

Validation:

- parked idle clients resume on frontend input;
- parked clients handle disconnect, cancel, terminate, timeout, and postmaster
  death correctly;
- byte-probe tests prove no-byte leaves message state untouched and
  byte-available pins the backend until the complete message is handled;
- byte-probe tests prove no-byte restores query-cancel holdoff, does not move
  receive-buffer cursors, and reports the correct transport wait mask;
- parked `LISTEN` sessions receive notifications;
- parked `idle in transaction` listeners do not spin or deliver notifications
  before transaction state permits it;
- timeout tests prove stale parked timeout generations do not fire detached
  timeout behavior;
- negative tests prove `pg_sleep()`, advisory locks, LWLocks/semaphores, and
  frontend output backpressure do not claim carrier release;
- process-mode and thread-per-session modes still work.

Exit gate:

- Phase 14 completion means the protocol park/resume foundation is correct and
  well tested. It does not require the final bounded carrier pool yet.

## Phase 15: Real Pooled Protocol Scheduler

Goal: turn the Phase 14 protocol-boundary foundation into a real carrier pool
where parked sessions do not own carriers and active commands lease carriers.

Design reference: `MULTITHREADED_PROTOCOL_SCHEDULER_DESIGN.md`.

Likely changes:

- Decouple client accept/session creation from carrier creation.
- Launch and manage a bounded carrier pool.
- Support validation runs with more client sessions than carrier threads.
- Split logical backend exit from physical carrier exit.
- Add session migration compatibility levels such as pooled-protocol-affine and
  pooled-protocol-migratable.
- Replace or split any single generic pooled-scheduler extension level before
  claiming session migration.
- Do not set the pooled protocol runtime requirement to the old
  `PG_BACKEND_MODEL_POOLED_SCHEDULER` marker; it is too coarse and ordinal
  loader compatibility would admit the wrong modules.
- Keep non-migratable sessions hard-affine or rejected from pooled protocol
  mode.
- Add migration/affinity policy after the compatibility split exists.
- Add scheduler observability for carrier count, running backends, parked
  protocol reads, runnable queue length, wake reasons, and migrated versus
  same-carrier resumes.
- Keep thread-per-session mode available for debugging and fallback.

Validation:

- many idle or think-time-heavy sessions run on fewer carriers;
- sessions outnumber carriers in at least one stress test;
- idle-in-transaction sessions can hold locks while carriers serve other
  sessions;
- no lost wakeups under frontend-input, timeout, notify, cancel, terminate, and
  disconnect races;
- carrier attach/detach invariant assertions are enabled in development builds;
- process-mode and thread-per-session modes still work.

Exit gate:

- Gate F is part of Phase 15 completion. Before leaving Phase 15, run the Gate
  F checks from the Test Strategy section: full process-mode and threaded-mode
  suites, focused protocol-scheduler TAP, sessions-greater-than-carriers
  stress, parked wake race tests, attach/detach invariant checks, and negative
  tests proving deep waits remain carrier-pinned.

Status: implemented; runtime-validated (2026-07-09, libxtc v1.9.0), Gate F TAP
005-009 now GREEN on a disk-backed host (2026-07-10, c7i.metal-24xl EC2, libxtc
v1.11.0).  Direct runtime evidence gathered
(pooled_protocol_carriers=N, multithreaded=on):

- Sessions outnumber carriers: 20 concurrent sessions each returning correct
  results on 2, 4, AND 8 carriers (ok=20 fail=0 in every case) -- real
  multiplexing, incl. 20-on-2.
- Idle-in-transaction hold: 6 sessions held in BEGIN...pg_sleep...COMMIT while
  only N carriers exist; a concurrent query still returns correctly and
  pg_stat_activity shows the pool multiplexing (fewer in-flight than sessions).
- Sustained throughput: pgbench -c 16 -j 4 -T 5 on 4 carriers -> ~800 tps, 0
  errors (process-mode baseline ~920 tps, same box/load).
- Cancel/terminate on a POOLED session: pg_cancel_backend wakes a parked
  pg_sleep (woke=yes); pg_terminate_backend removes it (gone=yes).
- No crash / clean shutdown: crash=0, fast stop clean, 0 cores in every run.
- Off by default (pooled_protocol_carriers=0 keeps thread-per-session); process
  mode unaffected.

Remaining before flipping the "experimental" label: (1) Gate F TAP 004-009 pass
on a disk-backed host (DONE 2026-07-10: 004 46, 005 35, 006 3, 007 46, 008 11,
009 33 subtests -- 009's pooled deep-wait pin bug fixed in 223663b9d93, the
stale-InitProcess-latch first-command collapse; 001/003/regress still fail on
the io_method=worker-expects-forked-io-workers vs multithreaded-remaps-to-xtc
conflict, a pre-existing test-vs-design mismatch tracked separately, not a Phase
15 blocker); (2) route the two pooled-path blocking poll() sites (postgres.c
~649 sticky-idle probe, ~6127 scheduler-carrier poll) through the xtc loop so
the pooled carrier never blocks on a raw poll; (3) enable the attach/detach
invariant asserts in a cassert run under stress.

## Phase 16: Bundled Extension Completion And Hardening

Goal: close Gate E2-Extensions after the core threaded runtime is working.
Threaded mode should become credible and complete for bundled in-tree modules,
procedural languages, and contrib extensions without delaying Phase 13
wait-observability or Phase 14/15 protocol-scheduler work.

Status: in progress (Session 4, 2026-07-10, commit 69c5f82c93f).  First bites
landed on top of the pooled-as-default flip:

- Procedural languages beyond PL/pgSQL, first audited pass:
  - pltcl -> POOLED_PROTOCOL_AFFINE (audited affine-safe: session-relocated
    interp/proc hashes + call state, explicit Tcl_Interp * on every call so no
    thread-global current-interp, set-once-read-only process init).  Loads and
    executes under both pooled default and thread-per-session.
  - plperl -> POOLED_PROTOCOL_AFFINE (per-(re)entry re-activation now landed:
    activate_interpreter() re-asserts PERL_SET_CONTEXT whenever the thread's
    actual current interpreter, PERL_GET_CONTEXT, no longer matches this
    session's interpreter -- closing the my_perl-drift hazard when sessions
    interleave on a carrier.  Validated on EC2 by TAP 012: 8 sessions on 2
    carriers, 25 interleaved rounds + mid-flight re-stamp + nested-SPI plperl,
    per-session isolation holds; plperl regression 14/14 unaffected.)
  - plpython -> PROCESS (defer-with-invariant: embedded CPython + PLy_* globals
    are process-global/GIL-serialized; needs per-session sub-interpreters or
    full relocation).
- Contrib backend-model batch: 18 stateless data-type / operator / dictionary /
  tablesample libraries marked POOLED_PROTOCOL_AFFINE after per-module
  mutable-state audit (citext, cube, hstore, intarray, isn, ltree, seg,
  btree_gin, btree_gist, pg_trgm, fuzzystrmatch, dict_int, dict_xsyn,
  earthdistance, unaccent, tablefunc, tsm_system_rows, tsm_system_time).  11
  validated loading + executing correct results under the pooled default.
- Session 3 already marked PL/pgSQL + the 21 encoding conversion procs +
  dict_snowball + libpqwalreceiver + regress.c affine.

Remaining Phase 16 work (Session 5 hardening + later):

- Likely work:

- migrate every contrib extension to explicit backend model metadata;
- make every contrib extension support thread-per-session mode by default;
- complete bundled procedural-language support beyond PL/pgSQL, or explicitly
  mark any temporary exception as process-only with a release-blocking note;
- finish custom/extension GUC ownership and hook semantics for threaded mode;
- add session/runtime APIs needed by contrib modules that currently rely on
  process-global mutable state;
- run contrib regression tests in process mode and threaded mode;
- document any temporary exception as a release-blocking gap rather than an
  unknown default;
- thread sanitizer runs where feasible;
- address sanitizer runs;
- stress tests for interrupts, waits, cancellation, and teardown;
- lock-order documentation for new runtime locks;
- debug views for runtime/backend/session/carrier state;
- crash and FATAL behavior tests;
- performance baselines.

Exit gate:

- Gate E2-Extensions / Gate G is part of Phase 16 completion and may need to
  run repeatedly during hardening. Before considering Phase 16 complete, run
  the Gate G checks from the Test Strategy section: feasible sanitizers,
  repeated full suites, threaded contrib regression for every contrib
  extension, bundled procedural-language checks, custom/extension GUC stress,
  crash/FATAL behavior tests, and performance baselines.

## Phase 17: Advanced Scheduler Boundaries

Goal: revisit more complex scheduler-yielding boundaries only after the
protocol-boundary scheduler is real, hardened, and measured.

This phase is intentionally post-hardening. Phase 14 and Phase 15 should not
depend on it, and Phase 16 hardening should be able to declare the
protocol-boundary scheduler release-ready without solving arbitrary deep waits.

Possible work:

- frontend output backpressure as a scheduler-yielding boundary;
- COPY input/output continuation states;
- selected lock waits with caller-specific continuation state;
- selected executor and utility yield points;
- AIO/storage completion boundaries where the upper stack can return to a
  known continuation;
- stackful coroutine/fiber research, only if the project deliberately chooses
  that direction;
- deeper extension compatibility levels such as task reentrancy.

Requirements before any boundary moves out of "carrier-pinned":

- exact call sites are identified;
- live C stack behavior is specified;
- continuation state is explicit and heap/session/execution owned;
- cleanup and error behavior is specified;
- retry semantics are correct;
- extension safety is understood;
- tests prove no detached backend has a live deep stack.

Validation:

- boundary-specific correctness tests;
- cancellation and timeout race tests;
- corruption-focused stress tests;
- performance comparison against the Phase 15 protocol scheduler;
- clear fallback to carrier-pinned behavior when a boundary is not safe.

## Phase 18: libxtc Deduplication And Fusion (performance lever)

Goal: once the runtime genuinely runs on libxtc, replace PostgreSQL functions
and hand-rolled threaded-runtime plumbing that reimplement primitives libxtc
already provides with thin wrappers over the xtc API -- both to shrink the
concurrency/runtime code PostgreSQL maintains itself AND, per the revised north
star, to CLOSE THE PERFORMANCE GAP with the fork model (Session-5 showed
threaded ~65 % of process, the cost being a layer of our own scheduling/
current-work indirection on top of libxtc plus duplicated pthread plumbing).

This phase was originally scoped as "deliberately last, audit-only."  With the
protocol-boundary scheduler (Phases 14-15) now real and the runtime owning
backends on xtc carriers, it is promoted to an ACTIVE performance lever and may
proceed in parallel with Phase 16/17.  Prioritize the adoptions with the best
perf/simplicity payoff first (see the revised north-star list below: xtc_svr/
xtc_orc/xtc_pool for the carrier + worker pools and supervision; the registry +
xtc_pg for backend registry and cross-fiber notify; xtc_xproc for the crash
watchdog; then Latch/LWLock/CV/AIO fusion).  Each adoption is A/B'd against the
hand-rolled version it replaces and must be neutral-or-better on
check-threaded-pooled perf before it lands; keep the fallback until proven.

### On the pthread <-> fiber boundary and test predictability

The runtime deliberately mixes explicit pthreads with libxtc concurrency.
Process mode is a permanently supported backend model, and the
postmaster/control-plane, single-user, bootstrap, and crash-escalation paths
are deliberate process-lifetime exceptions (see AGENTS.md).

### North star (revised 2026-07-11): go further with libxtc

The earlier framing ("full cutover to only libxtc is NOT a goal"; libxtc merely
a pluggable substrate under our own scheduler) is superseded by a more
aggressive goal.  Session-5 perf baselines showed threaded ~65 % of process
throughput on a few CPU-bound clients (thread-per-session 55k/82k vs process
84k/135k tps), the cost being PostgreSQL's own per-command scheduling +
current-work indirection layered ON TOP of libxtc rather than fused with it, and
our hand-rolled pthread plumbing (pooled-protocol queue mutex/cond, PMChild/GUC
locks, supervisor bookkeeping) duplicating what libxtc already does well.  The
hypothesis -- worth proving -- is that deeper libxtc integration CLOSES that gap
and may make the threaded branch FASTER than the fork model, by (a) removing a
layer of our own scheduling indirection and (b) using libxtc's tuned primitives
instead of parallel re-implementations.

Concretely, adopt the libxtc OTP behaviours (v1.10.0-v1.12.0 and beyond) as
first-class runtime building blocks rather than deferring them:

- xtc_svr / xtc_orc (supervisors, bounded pools, restart strategies): replace
  the hand-rolled per-loop supervisor fibers, the pooled-carrier pool
  bookkeeping, and worker restart policy.  The crash-aware registry reaper
  (v1.12.0) subsumes our orphan-reaper handshake.
- xtc_pool: back the pooled-protocol carrier pool and any bounded worker pool
  with the library's checkout/checkin pool instead of our own queue+mutex.
- xtc_fsm / xtc_svr handle_continue: model backend/session lifecycle and the
  protocol-park state machine as an explicit gen_statem-style FSM.
- registry + xtc_pg (process groups / pub-sub): replace the threaded backend
  registry and cross-fiber wakeup/notify fan-out (LISTEN/NOTIFY, proc-signal).
- xtc_xproc (cross-fork spawn/send/monitor, v1.12.0): the in-tree path for a
  separate watchdog/control process that outlives a carrier-process crash and
  re-execs the server -- the real answer to threaded restart_after_crash.
- xtc_credit: sliding-window regulation for admission/backpressure.
- Phase 18 dedup (Latch/LWLock/CV/AIO onto xtc primitives) moves from "last,
  audit-only" to an active performance lever, once the protocol scheduler is
  real (it now is).

This is still NOT a blind cutover: process mode stays supported, the
process-lifetime exceptions stand, and every adoption must keep gmake check +
check-threaded + check-threaded-pooled green and show a neutral-or-better perf
delta (A/B vs the hand-rolled version it replaces) before it lands.  Adopt one
behaviour at a time, measure, and keep the fallback until the replacement is
proven.  The governing question shifts from "can libxtc be a substrate" to
"where does fusing with libxtc make PostgreSQL faster and simpler than the fork
model."

Three layers coexist today (the surface the adoption above will consolidate):

- carrier layer: the xtc scheduler runs on a pthread pool (one loop per
  thread); regular client backends run as xtc fibers on those loops, but the
  non-fiber fallback, all aux worker families (bgworker, io worker, autovac,
  WAL, ...), and the pooled-protocol carriers are still raw pg_thread_create
  pthreads;
- synchronization: in-backend work uses PostgreSQL's shmem LWLock/spinlock/
  latch/CV (correct for both models), but the threaded-runtime plumbing still
  has raw pthread_mutex/pthread_cond guards (e.g. the pooled-protocol queue,
  malloc-trim, some PMChild/GUC locks);
- I/O: io_method=xtc routes fiber-backend data-file reads through xtc_aio;
  everything else still uses PostgreSQL's own AIO/pread and WAL paths.

Why this ordering matters for testing: nearly every hard bug so far
(concurrent lost-wakeup, shutdown wedge, worker-fiber terminate hang,
late-io-worker autoscale) lives at the pthread<->fiber boundary, where a
pthread-side SetLatch/signal must wake a fiber or a pthread reaper must observe
a fiber death. libxtc's Deterministic Simulation Testing (DST, docs/M_DST.md)
can replay scheduling bit-identically and inject faults, but ONLY for entities
on the xtc scheduler; every retained pthread is invisible to DST and
reintroduces real-hardware nondeterminism (the "lucky timing window" class of
flake). So the more of the runtime that moves onto xtc loops (fibers + xtc
primitives), the more of the system DST can make deterministic.

The correct response is NOT a big-bang cutover (moving raw pthread mutexes to
xtc CVs while the carriers are still pthreads is worse-of-both-worlds -- xtc
condition/mailbox semantics expect to run on a loop). Instead, shrink the
boundary in dependency order:

1. make fiber lifecycle robust first: Phase 13/AGENTS_XTC #7 Stage 1 adds
   fiber-death observation (xtc_monitor) so an abnormal fiber exit is seen and
   escalated;
2. then widen the fiber carrier to aux worker families (AGENTS_XTC #5) one at
   a time -- each family that moves off pg_thread_create onto an xtc loop
   removes a pthread and becomes DST-visible;
3. only then (here, in Phase 18) convert the now-fiber-resident raw
   pthread_mutex/pthread_cond plumbing to xtc primitives, because the xtc
   primitive is only correct once its users run on a loop.

Pulling the sync-primitive conversion earlier does not pay off until the
carriers are fibers, so the #7 -> #5 -> Phase 18 order is intentional and does
progressively make the test surface more DST-predictable.

Candidate duplication surfaces to audit (confirm against the current libxtc
public API in `XTC_ROOT/src/inc` before assuming an equivalence exists):

- latches and waits: `Latch`/`WaitEventSet`/`WaitLatch` versus xtc fiber
  parking (`xtc_proc_wait_fd`, wakers, mailbox sends). The xtc-carrier already
  routes `WaitEventSetWaitBlock` through `xtc_proc_wait_fd` while on a fiber;
  the audit asks which other latch/wait-event paths can collapse onto that
  seam instead of keeping a separate epoll/poll/kqueue implementation.
- lightweight locks and condition variables: `LWLock`, `ConditionVariable`
  versus `xtc_lwlock`, `xtc_lrlock`, `xtc_chan`, and mailbox signalling. Only
  where the xtc primitive preserves PostgreSQL's exact fairness, self-deadlock,
  interrupt-holdoff, and lock-ranking semantics.
- async I/O: PostgreSQL's `aio` method layer versus `xtc_aio`/`xtc_io`
  (io_uring/epoll/kqueue under one API). Audit whether the io-method worker
  path can dispatch through xtc's async file path instead of a parallel
  submission/completion engine.
- timers and timeouts: the `timeout.c` machinery versus xtc loop timers
  (`xtc_proc_sleep`, timer-driven wakeups). Only after Phase 5/13 timeout
  routing is logical and target-backend addressable.
- process/worker supervision: postmaster crash-restart and worker lifecycle
  versus xtc `xtc_orc` supervisor trees (one_for_one / one_for_all /
  rest_for_one / simple_one_for_one). This is the largest and riskiest
  candidate and stays gated behind the process-lifetime exceptions in
  `AGENTS.md`.
- memory contexts and allocators: only note, do not act, unless a concrete
  win appears. PostgreSQL's memory-context invariants are load-bearing and
  `xtc_mctx`/`xtc_alloc_audit` equivalence must be proven, not assumed.

### Memory subsystem and shared memory (deferred to this phase, with reasons)

This is called out explicitly because it is tempting to reach for the libxtc
allocators early. Both are Phase 18 items, not earlier, and one is a non-goal:

- Heap memory (`MemoryContext` -> `xtc_mctx`): libxtc v1.2.0 exposes
  `xtc_mctx_*` -- a hierarchical memory-context allocator (create/reset/
  destroy/alloc/calloc/strdup/free, parent-child, register_cleanup,
  total_bytes accounting) that is structurally the same model as PostgreSQL's
  `MemoryContext`. This makes it a real dedup candidate, but porting the
  backend's memory subsystem is enormous and load-bearing: every `palloc`,
  the aset/slab/generation/bump allocators, reset-callback ordering, the
  `CurrentMemoryContext` current-cell, and error-path context switching all
  depend on exact semantics. It must stay `keep`/`defer` until (a) the runtime
  is proven on carriers and (b) a per-allocator equivalence harness shows
  `xtc_mctx` matches PostgreSQL's alignment, chunk-header, reset, and
  callback semantics. Do NOT convert it as a prerequisite for anything else.
  Note: carrier-layer plumbing that runs BEFORE PostgreSQL memory init (e.g.
  the xtc carrier's fiber spawn path) cannot use `palloc` at all; that code
  must be allocation-free or use a libxtc/OS primitive, and is out of scope
  for the MemoryContext port.

- Shared memory: converting PostgreSQL's shared-memory infrastructure to
  libxtc is a NON-GOAL. libxtc deliberately does not own or replace DSM/
  `dynamic_shared_memory`; per libxtc's own M16 adapter guidance, PostgreSQL
  owns the shared-memory segment and lifecycle, and the only sanctioned
  integration is placing an `xtc_slab` allocator (SHARED_MEMORY mode, via
  `xtc_slab_opts_t.shm_base`/`shm_size`) INSIDE an already-PG-allocated DSM
  region -- reusing the same mmap primitives without a second
  segment-tracking layer. So the audit row is: keep PostgreSQL's shmem
  segment/DSM ownership as-is; the optional, opt-in `wrap` is `xtc_slab` on
  top of a PG-owned region where a slab allocator is a net win, nothing more.
  Rewriting `pg_shmem`/DSA/DSM onto libxtc is explicitly rejected.

Method:

- produce a checked-in inventory mapping each PostgreSQL primitive to its
  nearest xtc API, with a verdict per row: `replace`, `wrap`, `keep`
  (semantics diverge), or `defer` (needs a later phase). Record the guard that
  would catch a wrong equivalence assumption for every `replace`/`wrap` row.
- prefer `wrap` (PostgreSQL API unchanged, xtc under the covers) over changing
  call sites, so the process-mode fallback and existing callers stay intact.
- keep process mode green: any wrap must still have a correct non-xtc path,
  since process-mode PostgreSQL remains a supported backend model.
- one primitive family per commit, each with a focused equivalence test that
  fails if the xtc-backed path diverges from the PostgreSQL semantics it
  replaced.

Explicit non-goals:

- do not replace a PostgreSQL primitive merely because an xtc analogue exists;
  a `keep` verdict with a one-line semantic-divergence reason is a valid,
  common outcome;
- do not touch single-user, bootstrap, frontend-utility, or crash-escalation
  paths, which remain process-lifetime exceptions;
- do not overfit to WASM or any single host runtime.

Validation:

- process-mode and threaded-mode full suites stay green across each wrap;
- per-family equivalence tests (fairness, wakeup, cancellation, timeout,
  deadlock, error-recovery) for every `replace`/`wrap`;
- no measurable regression versus the pre-audit baseline, or an explicit
  documented tradeoff if a wrap trades throughput for less maintained code.

Exit gate:

- the inventory is complete and every row has a verdict and (for
  replace/wrap) a guard;
- landed wraps keep both process mode and threaded mode green;
- remaining duplication is either intentionally `keep`/`defer` with a recorded
  reason, not an unaudited accident.

## Phase 19: Process-Fallback Backend For Incompatible Extensions

Goal: turn the extension-model gate from a *fail-closed wall* into a
*fail-safe route*. Today an extension that cannot run in a threaded/pooled
carrier (default `PG_BACKEND_MODEL_PROCESS`, i.e. every unmarked third-party
module) is rejected with an `ERROR` at load time. That satisfies
defense-in-depth but not compatibility: a user who installs a legacy
process-only extension in a `multithreaded=on` server currently loses that
extension entirely. Phase 19 keeps the hard defensive gate but adds a
compatibility escape hatch: a session that needs a process-only extension runs
in a dedicated **forked, exec'd, supervised process backend** instead of on a
carrier fiber, transparently to the client.

This is the "both defense in depth and compatibility" requirement: the carrier
runtime never loads unsafe code into the shared address space (defense), and
the extension still works (compatibility) by running where it is safe -- an
isolated process.

### What exists today (audited 2026-07-12)

- Default module model is `PG_BACKEND_MODEL_PROCESS` (fmgr.h): unmarked/legacy
  modules are process-only by construction, so they are exactly the population
  Phase 19 must route.
- The gate is fail-closed: `dfmgr.c:check_module_backend_model` +
  `module_backend_model_is_compatible` raise `ERROR`, `dlclose`, and refuse the
  library when `module_model < required_model`, both at first load
  (`load_external_function` / `internal_load_library`) and at every runtime
  model transition (`check_loaded_modules_backend_model`, called from
  `PgRuntimeSetExtensionBackendModel`).
- Each runtime advertises what it *requires* of extensions:
  `process_runtime -> PROCESS`, thread-per-session -> `THREAD_PER_SESSION`,
  pooled -> `POOLED_PROTOCOL_AFFINE` (backend_runtime.c). The demanded model is
  read at load time via `PgRuntimeGetExtensionBackendModel()`.
- The postmaster already routes fork-vs-carrier per child in
  `postmaster_child_launch_carrier` (launch_backend.c). Client backends under
  pooled mode go to `postmaster_pooled_protocol_launch`; the classic
  fork path is `postmaster_child_launch` -> `internal_forkexec` (EXEC_BACKEND)
  or `fork_process` (non-EXEC_BACKEND).
- Crucial constraint, already encoded in the tree: *once any thread carrier
  exists, later fork-WITHOUT-exec is unsafe* -- `postmaster_child_launch_carrier`
  returns `ENOSYS` for that case. A process-fallback backend therefore MUST use
  **fork + exec** (`internal_forkexec` / `save_backend_variables` /
  `SubPostmasterMain`), never bare `fork_process()`, so the child starts from a
  clean single-threaded address space with no inherited locked mutexes.
- There is no existing "escalate this session to a process" path. Phase 19 is
  net-new routing + detection; the enforcement primitives it builds on already
  exist.

### The hard problem: detection timing

Extensions become known to a backend at several points, and most are too late
to fork cleanly (the session is already a running carrier fiber mid-command):

1. `shared_preload_libraries` -- known at postmaster start, global, before any
   client. (Easy: if any preloaded lib is process-only, the whole server is
   process-only anyway; handled by existing gate at startup.)
2. `local_preload_libraries` / `session_preload_libraries` -- known at session
   startup, from GUCs, *before* the first command runs. (Good hook point.)
3. `LOAD 'lib'` -- explicit, runs as a command inside an already-placed session.
4. `CREATE EXTENSION` / first call into an extension function -- lazy load via
   `fmgr` deep inside command execution, the worst case: the session is already
   on a carrier fiber and fork-clean is impossible from there.

The design principle: **decide the backend model at the session-placement
boundary, from metadata, before the session is bound to a carrier** -- not at
the dlopen deep in fmgr. Cases (1)-(2) are decidable at placement. Cases
(3)-(4) need one of:

- (preferred) a **catalog-driven pre-declaration**: `pg_extension` /
  a new `pg_extension_backend_model` mapping (or a column) records each
  installed extension's model, populated at `CREATE EXTENSION` time by reading
  the control file / probing the module magic in a throwaway process. At session
  placement the scheduler consults the catalog for the extensions the session is
  entitled to use (search_path, installed extensions) and routes
  conservatively; or
- (fallback) a **lazy re-placement**: when `fmgr`/`dfmgr` hits an incompatible
  module mid-command on a carrier, instead of `ERROR` it raises a distinct
  internal condition that unwinds the command cleanly and re-dispatches the
  session as a process backend (a "needs-process" retry). This is the
  compatibility-of-last-resort path; it costs one aborted command + a
  reconnect-like re-placement, but it is transparent to correctness (the
  command had not committed -- the incompatible load is detected before the
  extension's code runs).

### Proposed model

- Add a session backend-model *demand* that starts at the runtime default
  (pooled/thread) and can only ever be **downgraded** toward `PROCESS` as
  process-only extension needs are discovered. Downgrade is monotonic and
  sticky for the session's life.
- At session placement (`postmaster_pooled_protocol_launch` /
  thread-per-session launch), if the session's known demand is already
  `PROCESS` (from preload GUCs or catalog pre-declaration), route it straight to
  `postmaster_child_launch` (fork+exec) as a **process-fallback backend**
  instead of enqueuing it on the carrier pool.
- If the demand is discovered late (case 3/4), unwind the current command
  without committing and re-place the session as a process-fallback backend.
  This requires the protocol/wait boundary to support a "detach and hand back to
  postmaster for process launch" transition; scope this behind Phase 17
  (advanced scheduler boundaries) if the clean-unwind machinery is not yet
  available, and until then keep case 3/4 as the existing `ERROR` (documented,
  fail-closed, no silent corruption).
- Process-fallback backends are **supervised** exactly like classic process
  backends: they are `PMChild`ren, counted against `MaxConnections`, reaped by
  the postmaster, and subject to the same crash-restart policy as process mode
  (they do NOT share the corruptible carrier address space, so a crash in one
  is a normal single-backend crash, not a fail-stop of the whole server -- a
  strictly better isolation story than an in-carrier extension crash). This is
  the user's "spawn/monitor a backend" requirement, satisfied by the existing
  postmaster child supervision.

### Defense in depth (unchanged, layered)

- Layer 1 (still primary): the fail-closed model gate. Unsafe code is NEVER
  dlopen'd into a carrier address space. Phase 19 only adds a *route*, it does
  not weaken the gate; an extension marked process-only still cannot load on a
  carrier fiber.
- Layer 2: monotonic session-demand downgrade -- once a session is known to need
  a process, it can never be re-promoted onto a carrier.
- Layer 3: catalog pre-declaration means the common case is decided before any
  untrusted code runs.
- Layer 4: the last-resort lazy re-placement detects the incompatibility at
  dlopen, i.e. before the extension's functions execute, so a mis-declared
  extension still cannot run in a carrier.

### Feasibility finding (audited 2026-07-12): fork+exec machinery is Windows-gated

The clean fork+exec child-launch path (`internal_forkexec`,
`save_backend_variables` / `restore_backend_variables`, `SubPostmasterMain`)
exists in `launch_backend.c` but its call sites are wrapped in
`#ifdef EXEC_BACKEND`, and `EXEC_BACKEND` is defined only on Windows
(`pg_config_manual.h`: `#if defined(WIN32) && !defined(__CYGWIN__)`). On Linux
the live child launch is bare `fork_process()` (no exec) -- exactly the path the
carrier code already refuses (`ENOSYS`) once carriers exist, because
fork-without-exec in a multithreaded process is unsafe.

Consequence for Phase 19: the process-fallback route on Linux CANNOT reuse the
default Linux launch path; it must bring the fork+exec machinery in even in a
non-`EXEC_BACKEND` build, scoped to the fallback children only. Concretely,
either (a) compile `internal_forkexec` + `save/restore_backend_variables` +
`SubPostmasterMain` unconditionally (they are mostly already unguarded as
functions; only the *call site* and a few helpers are `#ifdef`'d) and add a
fallback-only launch entry that always fork+execs, or (b) gate a new
`USE_XTC_PROCESS_FALLBACK` that pulls in the same machinery. This is postmaster
child-launch hot-path work: it must be validated end-to-end on a real pooled
threaded server (a mistake breaks all backend startup), so the implementation
slice is EC2-gated, not a local-only change. The pre-placement detection and
the monotone session-demand field can be prototyped locally ahead of it.

### Likely changes

- `dfmgr.c`: split "incompatible" into "reject" (marked incompatible, genuine
  error) vs "needs-process" (default/process-only module in a threaded session),
  and surface the latter as a routable condition instead of only `ERROR`.
- Session placement (`launch_backend.c`): a `PG_BACKEND_LAUNCH_PROCESS_FALLBACK`
  route that forks+execs a supervised process backend for a session whose demand
  is `PROCESS`, coexisting with the carrier pool.
- A session backend-model demand field on the logical backend/runtime, monotone
  toward `PROCESS`, seeded from preload GUCs and (later) catalog metadata.
- Optional catalog pre-declaration of per-extension backend model, populated at
  `CREATE EXTENSION` by probing the module magic in an isolated process.
- Metrics: count process-fallback sessions, late re-placements, and reject vs
  fallback outcomes, so operators can see how much of their workload cannot run
  threaded.

### Validation

- a process-only test extension used by a `multithreaded=on` session runs
  correctly in a process-fallback backend (not rejected), while the same
  extension marked threaded runs on a carrier;
- preload-GUC and catalog-pre-declared process-only extensions are routed to a
  process backend at placement, never touching a carrier;
- a mis-declared extension is still refused from a carrier (defense holds);
- process mode is byte-for-byte unaffected (no fallback path taken);
- a crash inside a process-fallback backend is a normal single-backend crash,
  not a whole-server fail-stop;
- fork+exec is used (never fork-without-exec) once carriers exist -- assert the
  `ENOSYS`-guarded invariant still holds;
- the late re-placement path (if built) aborts the in-flight uncommitted command
  and transparently continues the session as a process backend.

### Sequencing and dependencies

- The fork+exec process backend and preload-GUC/catalog detection are
  independent of the deep-unwind re-placement; ship the pre-placement route
  first (covers preload + catalog cases, the majority), keep case 3/4 as the
  existing fail-closed `ERROR` until the clean-unwind machinery from Phase 17
  exists, then add lazy re-placement.
- Phase 16 owns the model metadata for bundled modules; Phase 19 owns the
  *routing* for everything the gate still rejects. Phase 19 can proceed once
  pooled placement (Phase 15) is real, which it now is.

### Progress

- **[done] Increment 1 -- classification.** `incompatible_module_backend_model_error`
  now distinguishes process-only modules (the default/legacy population the
  fallback will serve) from mis-declared threaded-but-weaker modules, with an
  actionable operator message + hint for the former ("not supported in the
  threaded backend runtime"; run `multithreaded=off`, or adapt+mark). Gate stays
  fail-closed. Validated: test_extensions regression 4/4. Commit `66dc4d18674`.
  This is the classification the fork+exec route will branch on.
- **[DONE] Increment 2 -- fork+exec route (the meaty part).** Landed as a
  series: 2(a) compile the fork+exec machinery on Linux under a new
  `USE_XTC_PROCESS_FALLBACK` / `FORKEXEC_BACKEND` condition, process mode
  byte-identical (a074a3ae042, f996910054a); 2(c) the
  `PG_BACKEND_LAUNCH_PROCESS_FALLBACK` route from
  `postmaster_pooled_protocol_launch` behind the `xtc_force_process_fallback`
  test knob, plus all exec'd-child state restore gated on
  `PG_BACKEND_WAS_FORKEXECED` so only the exec'd child re-derives -- io_method
  remap, shmem re-attach, hba/ident re-derivation, DSM control attach
  (dbae3d31965, 1a12a49ab81); 2(e) crash contract pinned by TAP 013. Validated
  end-to-end on EC2: a client connection runs as a fork+exec'd process backend
  (SELECT + table round-trip); safety matrix (process / threaded-fallback-off /
  threaded-fallback-on) green.
  - 2(e) reframed: a process-fallback backend crash SHOULD fail-stop (same as a
    carrier crash) -- it is isolated at the address-space level but NOT the
    shared-memory level, and under multithreaded=on the postmaster cannot safely
    SIGQUIT+reinit shared memory with live carriers. TAP 013 pins fail-stop +
    committed-data-survives + external-restart (7/7 on EC2).
  - Known nuisance (Bug A, open): config-file GUC lines placed AFTER
    `multithreaded` are dropped during startup; workaround = order
    fallback/sysv before multithreaded (013 does this). Root cause not yet
    pinpointed (in the threaded-GUC apply path); low severity.
- **[DONE] Increment 3 -- detection (server-wide slice).** Under
  `multithreaded=on`, process-only `shared_preload_libraries` are re-checked
  against the carriers' demanded model after `process_shared_preload_libraries`
  and REJECTED at startup with the Increment-1 message, instead of silently
  dlopen'ing into the shared carrier address space (23e99a9b159). Validated:
  multithreaded=off loads; multithreaded=on refuses. Also: io workers never
  start under multithreaded=on (f49e0dcf5bd). Per-SESSION detection +
  mid-session re-placement remains gated on Phase 17 (the postmaster cannot see
  per-session preload GUCs at placement; re-placing a live carrier session is
  unsafe until the clean-unwind machinery exists).
- **[after Phase 17] Increment 4 -- lazy re-placement** for late-discovered
  incompatibility (LOAD / CREATE EXTENSION / first fmgr call): abort the
  uncommitted command and re-place the session as a process backend. Needs the
  Phase-17 clean-unwind machinery; until then case 3/4 stays fail-closed ERROR
  (now with the Increment-1 actionable message).

## PL/pgSQL And In-Tree Modules Plan

PL/pgSQL should be the first nontrivial module to support threaded mode.

Approach:

- audit global and static state in `src/pl/plpgsql`;
- classify caches as session-local, runtime-global immutable, or synchronized;
- move mutable session caches into `PgSession` extension state or PL/pgSQL's
  own session-owned object;
- keep process mode behavior unchanged;
- mark PL/pgSQL thread-per-session safe only after tests pass.

Contrib modules should be handled after the mechanism is proven, but they are a
required end-state deliverable:

- start with simple stateless modules;
- reject or defer modules with background workers, process-global caches, or
  unsafe external library assumptions until the required APIs exist;
- document each opt-in;
- by the final hardening phase, every contrib extension should support
  thread-per-session mode and have explicit backend model metadata;
- final gates should not pass with unknown/default process-only contrib modules.

## Test Strategy

Process mode remains the control group. Testing should be tiered so routine
development stays fast, while broader suites run before each increase in risk.

Every commit should run:

- build for touched targets, at minimum the backend when backend code changes;
- focused tests for the files or subsystems touched;
- `git diff --check`.

Every phase end should run:

- backend build;
- core regression tests;
- targeted TAP tests for touched areas;
- isolation tests when lock, wait, transaction, or cancellation behavior is
  touched;
- extension load tests once extension gating exists;
- PL/pgSQL tests once PL/pgSQL is in scope.

Full-suite gates should run after groups of phases, not after every phase.
These gates should use `check-world` or a documented near-equivalent when local
platform/tooling issues make literal `check-world` noisy.

Phase 12 closeout verification:

- documentation-only closeout commits: `git diff --check`;
- final Gate E2-Core verification: `gmake check`, `gmake check-threaded`,
  `gmake check-threaded-workers`, `gmake check-threaded-world-core`,
  `gmake check-runtime-lifecycles`, `gmake check-global-lifetimes`, and
  `git diff --check`;
- if any Phase 12 guard regresses, reopen only the evidence-driven blocker and
  preserve the defer-with-invariant split for Phase 16 extension/language/GUC
  completeness.

Phase 13 validation cadence:

- wait-boundary source slices: touched-object build, focused wait/latch/timeout
  tests, and `git diff --check`;
- lock, latch, condition-variable, timeout, frontend input, or frontend output
  observability changes: targeted threaded TAP plus isolation tests where
  relevant;
- wait-completion changes: preserve the blocking thread-per-session fallback
  and run cancellation, termination, idle timeout, transaction timeout, and
  reconnect coverage;
- do not treat Phase 13 validation as evidence that a wait family can release a
  carrier.

Gate A, after Phase 3:

- main-loop boundary and session scaffolding are complete;
- run core regression, isolation tests where relevant, and targeted
  protocol/error-recovery tests.

Gate B, after Phase 6:

- logical interrupts, timeout routing, and backend lifecycle/exit are complete;
- run `check-world` or close to it, plus focused cancellation, timeout,
  config reload, LISTEN/NOTIFY, and disconnect/FATAL tests.

Gate C, after Phase 8:

- extension gating and the thread-safety floor are complete;
- run `check-world`, static global report checks, extension load tests using
  the test-only threaded backend model, and PL/pgSQL process-mode regression
  tests;
- reject the gate if the static global report still contains unsafe
  unclassified globals from the Phase 8 required floor.

Gate D, after Phase 10:

- first thread-per-session runtime exists for regular client backends;
- run full process-mode tests and the threaded smoke/regression subset:
  multiple concurrent clients, running-query cancellation, idle and active
  backend termination, `ERROR` recovery, transaction abort cleanup, PL/pgSQL
  smoke tests, incompatible extension rejection, and repeated
  connect/disconnect stress;
- verify late server-owned worker subprocess launches are blocked or deferred
  after backend thread carriers exist;
- explicitly document which in-tree worker families remain startup-time process
  carriers, disabled, or deferred until Phase 11.

Gate E, after Phase 11:

- normal threaded server mode no longer forks in-tree server-owned workers
  after runtime startup;
- run threaded worker smoke tests for autovacuum, checkpointer, background
  writer, WAL writer, archiver, syslogger, WAL receiver, WAL summarizer,
  startup/recovery, physical basebackup/hot-standby promotion, logical
  replication workers, AIO workers, and any in-tree generic background workers
  that explicitly opt into the thread backend model;
- verify worker cancellation, shutdown, restart, and failure escalation;
- verify single-user, bootstrap, frontend utility, postmaster/control-plane,
  and crash-escalation paths remain documented process-lifetime exceptions;
- run full process-mode tests and the threaded-mode worker subset.

Gate E2-Core closeout:

- core thread-per-session lifecycle and state ownership are coherent enough to
  start wait-observability and protocol-boundary scheduler work;
- run `gmake check-runtime-lifecycles` and `gmake check-global-lifetimes`;
- run a full build, focused process-mode backend-runtime regression, direct
  threaded runtime TAP, PL/pgSQL coverage, and focused core regression smokes
  for GUCs, teardown, cancellation, termination, reconnect, and worker
  handoff;
- verify the threaded TAP log guard has no crash/corruption signatures and no
  retained `TopMemoryContext` accounting warnings;
- verify process-only extensions/background workers are still rejected in
  threaded mode and PL/pgSQL still works;
- do not require contrib-wide threaded regression, bundled languages beyond
  PL/pgSQL, or the full custom/extension GUC matrix here. Those are
  Gate E2-Extensions / Gate G work in Phase 16.

Gate F, after Phase 15:

- the protocol-boundary scheduler is real: sessions can outnumber carriers,
  parked top-level protocol reads detach from carriers, and deep waits remain
  carrier-pinned;
- run full process-mode and threaded-mode suites, focused protocol-scheduler
  TAP, sessions-greater-than-carriers stress, parked wake race tests for
  frontend input, disconnect, cancel, terminate, timeout, `LISTEN`/`NOTIFY`,
  and postmaster death, plus negative tests for `pg_sleep()`, advisory locks,
  LWLocks/semaphores, and frontend output backpressure.

Gate G, during and before completing Phase 16:

- hardening and release-readiness gate;
- run sanitizers where feasible, repeated full suites, threaded contrib
  regression tests for every contrib extension, bundled procedural-language
  checks, custom/extension GUC stress, stress tests for
  interrupts/waits/cancellation/teardown, crash and `FATAL` behavior tests, and
  performance baselines.

Gate H, during any Phase 17 advanced scheduler-boundary work:

- advanced-boundary research gate;
- each newly carrier-yielding boundary must have boundary-specific correctness,
  cancellation, timeout, cleanup, error-recovery, extension-safety, and stress
  coverage before it is treated as more than experimental.

## Risk Register

### Top-Level Error Recovery

Risk: moving `PostgresMain()` state breaks `ERROR` recovery or protocol sync.

Mitigation:

- preserve the always-active top-level `sigsetjmp` boundary initially;
- make `PgSessionStep()` the protected public entrypoint;
- keep unprotected helpers private;
- extract recovery code with minimal semantic changes;
- add targeted protocol error tests.

### Hidden Mutable Globals

Risk: thread mode corrupts session state through unclassified globals.

Mitigation:

- introduce the runtime/session/backend object vocabulary before broad
  classification;
- use Phase 4 to establish a baseline rather than requiring full-tree
  classification immediately;
- annotate or isolate the Phase 8 required floor before thread launch;
- use static reports;
- reject unclassified mutable globals in new code;
- prefer thread-local transition wrappers before object migration;
- keep shrinking the remaining baseline through later migration and contrib
  phases until all relevant mutable globals are classified or isolated.

### Lost Wakeups

Risk: replacing signals/latches introduces race conditions.

Mitigation:

- use atomic interrupt bits;
- document clear-before-check wait patterns;
- stress wait/cancel paths;
- borrow proven patterns from Heikki's interrupt work.

### Extension Unsafety

Risk: arbitrary extensions use mutable statics or unsafe libraries.

Mitigation:

- default extensions to process-only;
- add explicit module metadata;
- distinguish thread-per-session safety from pooled-protocol migration safety;
- keep non-migratable sessions hard-affine, process-only, or rejected from
  pooled protocol mode;
- provide session-state APIs;
- migrate PL/pgSQL and selected in-tree modules first;
- for extensions that cannot run threaded at all (the fail-closed gate's reject
  population), route the *session* to a forked+exec'd supervised
  process-fallback backend rather than losing the extension -- defense (never
  dlopen unsafe code into a carrier) plus compatibility (the extension still
  runs, in isolation). See Phase 19.

### `PGPROC` Ownership

Risk: `PGPROC` currently conflates backend identity, lock waiting, proc array
membership, transaction visibility, and wakeup/latch identity.

Mitigation:

- in early thread-per-session mode, keep one `PGPROC` per logical backend;
- in Phase 14/15 protocol scheduling, keep `PGPROC` owned by the logical
  backend while carriers only borrow it during attach;
- assert attach/detach invariants for `MyProc`, `MyLatch`, `procLatch`,
  `FeBeWaitSet`, wait-event fields, and current pointers;
- keep deep waits that place `PGPROC` on wait queues carrier-pinned;
- only later split execution/transaction leasing from idle session identity.

### File Descriptor Accounting

Risk: per-session fd caches in one process exceed process-wide fd limits.

Mitigation:

- add runtime-level fd budget accounting before threaded mode is broadly used;
- keep virtual fd state session-local until sharing is audited.

### Cache Sharing

Risk: making relcache/catcache shared too early creates subtle races.

Mitigation:

- keep cache state session-local first;
- rely on existing invalidation semantics;
- optimize sharing later only with clear locking rules.

### Signal Semantics

Risk: process-level signals do not map cleanly to threaded backend identities.

Mitigation:

- signals become external delivery only;
- backend-to-backend communication uses logical interrupts;
- thread mode avoids per-backend Unix signal handling.

### Backend Exit

Risk: a threaded backend follows a process-exit path and terminates the whole
runtime or leaves backend-local resources attached.

Mitigation:

- split backend exit from process exit before thread launch;
- route exit callbacks through backend-aware cleanup;
- reserve process termination for process mode, postmaster death, or `PANIC`;
- stress repeated connect/disconnect and `FATAL` paths.

### Auxiliary Worker Process Dependence

Risk: threaded mode proves client sessions but still depends on forked
auxiliary workers, leaving the server only partially threaded in normal
operation.

Mitigation:

- keep Phase 10 scoped to regular client backends and make that limitation
  explicit;
- add Phase 11 as the no-fork normal-mode worker milestone;
- give worker families their own runtime owner instead of pretending every
  worker is a user session;
- keep single-user, bootstrap, frontend utility, postmaster/control-plane, and
  crash-escalation paths as documented process-lifetime exceptions;
- require extension metadata before third-party background workers can opt into
  threaded worker execution.

## Suggested Commit Sequence From Phase 13

1. Finalize and record the Gate E2-Core validation baseline if it has not
   already been recorded on the branch.
2. Preserve or restore a clean Phase 13 wait-observability baseline. Deep waits
   may publish wait-completion records, but must not imply carrier detach.
3. Record the Phase 13 interrupt/latch boundary from
   `MULTITHREADED_PHASE13_PLAN.md` in any new wait or scheduler helper API:
   logical events are interrupts, wait readiness is latch/CV/wait-completion,
   and process lifecycle remains process signalling.
4. Link Phase 14 and Phase 15 work to
   `MULTITHREADED_PROTOCOL_SCHEDULER_DESIGN.md`.
5. Remove, disable, or assert-unreachable generic scheduler requeue hooks from
   deep wait-completion records before adding new pooled scheduler behavior.
   This is a hard Phase 14A.0 gate.
6. Add the protocol byte-probe primitive with explicit no-byte, byte-available,
   and EOF/error semantics, plus tests that prove no-byte does not advance
   buffer or message-read state, does not leave query-cancel holdoff elevated,
   and reports transport read/write/buffered-input readiness. Buffered transport
   input must return an immediately consumable byte or requeue for immediate
   re-probe; it must not sleep waiting for kernel socket readiness.
7. Add explicit protocol-park prepare/commit APIs, including parked wake reason,
   generation/sequence tracking, and deferred-notify generation tracking.
   `PgSessionStep()` prepares the park and returns; the carrier loop commits
   detach only after the step stack has unwound.
8. Extend `PgStepResult` and backend-exit paths so protocol park, normal logical
   exit, and fatal logical exit return to the scheduler before dispatch grows.
9. Add backend-indexed timeout snapshot/wake support, or require reattach before
   inspecting/firing timeout state that depends on current-backend globals.
   Include timeout generation validation for stale parked snapshots.
10. Decide and assert the Phase 14 `PGPROC`, latch, logical wake object, and
   `FeBeWaitSet` ownership rules.
11. Define the concrete parked wake routing table across frontend transport,
    `SendInterrupt()`, proc-signal fallback, `PGPROC->procLatch`, timeout, and
    postmaster death.
12. Teach `PgSessionStep()` to return a prepared protocol-read park result only
    before any new frontend message byte has been consumed.
13. Cover frontend input, disconnect, cancel, terminate, timeout, postmaster
    death, and `LISTEN`/`NOTIFY` wakeups.
14. Add negative tests proving `pg_sleep()`, lock waits, LWLocks/semaphores, and
   frontend output backpressure remain carrier-pinned.
15. Add attach/detach invariant assertions for current pointers, TLS mirrors,
    `PGPROC`, latches, `FeBeWaitSet`, memory contexts, resource owners,
    timeouts, and scheduler state.
16. Split extension compatibility into thread-per-session,
    pooled-protocol-affine, pooled-protocol-migratable, and later
    task-reentrant levels before any migration claim.
17. Split PMChild logical-backend publication from physical carrier-thread
    lifetime before claiming a reusable carrier pool.
18. Decouple client sessions from carrier creation and add a bounded carrier
    pool.
19. Add sessions-greater-than-carriers stress coverage and soft carrier
    affinity/grace-pinning instrumentation.
20. Run Gate F before leaving Phase 15.
21. Defer contrib-wide threaded support, bundled languages beyond PL/pgSQL, and
    the full custom/extension GUC matrix to Phase 16.
22. Defer frontend-output yielding, COPY continuations, lock-wait yielding,
    executor/utility yield points, and AIO/storage scheduler boundaries to
    Phase 17.

Each commit should leave process mode buildable. Prefer temporary compatibility
wrappers to broad all-at-once rewrites.

### Phase 17 re-scoping note (2026-07-14, after the Phase-18 parity close-out)

Before implementing Phase 17, a code re-read clarified that its original framing
was written from a partly STACKLESS mental model that does not match the shipped
STACKFUL fiber reality, and this materially narrows the phase:

- A fiber blocked deep in the C stack (LWLockAcquire wait, lock-manager
  ProcSleep->WaitLatch, ConditionVariableSleep, pg_sleep) ALREADY yields its
  carrier today: WaitEventSetWait/WaitLatch route through xtc_pg_wait_fd when in
  a backend fiber, so the fiber parks and the carrier runs its OTHER fibers.  The
  fiber's C stack is preserved automatically (it IS the fiber) -- so the Phase 17
  requirements "continuation state is explicit and heap/session/execution owned"
  and "no detached backend has a live deep stack" are stackless-model artifacts;
  with stackful fibers the live stack is fine and needs no explicit continuation.
- What "carrier-pinned" actually means in the shipped design (per 009 + the
  Phase-15 notes): during active command execution a session does NOT
  PROTOCOL-PARK (release its pool slot for a DIFFERENT session to take the
  carrier). It still yields the fiber; it just does not hand its carrier slot
  back to the pool mid-command.
- So the real, narrower Phase 17 question is: should a session that is
  deep-waiting mid-command (e.g. a long lock wait) RELEASE its carrier pool slot
  so another queued session can use that carrier, then resume later (possibly on
  a different carrier)?  This is a SCHEDULER-ACCOUNTING change (slot leasing
  across a mid-command wait), NOT a "capture the C stack" continuation problem.
  And its value is bounded: a carrier already multiplexes many fibers, so it only
  matters when most/all fibers on a carrier are simultaneously deep-waiting
  (few carriers, many long lock waits) -- a real but narrow starvation case.

Practical implication: Phase 17 should be re-specified around (a) mid-command
carrier-slot leasing for long waits (measure the starvation case first -- is it
real at the auto-tuned carrier count?), and (b) the genuinely-still-blocking
syscalls that do NOT go through xtc_pg_wait_fd (audit those; they are the true
carrier-monopolizers).  It is NOT the stackless-continuation rewrite the original
text implies.  Requirement rewrite + a starvation micro-benchmark (does a carrier
with all-deep-waiting fibers actually stall throughput?) should precede any code.
Also: Phase 19 Increment 4 (lazy re-placement) needs a clean mid-command unwind
regardless, which IS real work here.

### Phase 17 audit result: the carrier-monopolizer is ProcWaitOnSemaphore (2026-07-14)

Per the re-scoping note, audited for backend-path waits that do NOT go through
the fiber-aware seam (WaitEventSetWaitBlock -> xtc_pg_wait_fd).  Found the single
concrete target:

  ProcWaitOnSemaphore(proc, wait_event)  [src/backend/storage/lmgr/proc.c:2148]
    -> ProcSemaphoreWaitCallback -> PGSemaphoreLock(proc->sem)
    -> raw sem_wait() [posix_sema.c:313]  -- BLOCKS THE CARRIER OS THREAD.

Its own comment already flags this: "The actual wait remains carrier-pinned in
PGSemaphoreLock()" and "semaphore waits remain carrier-pinned until a later
explicit deep-wait continuation design exists" -- that later design IS Phase 17.

Callers (all hot-path, mid-command deep waits):
  - lwlock.c x4 (LWLock acquire slow path -- the big one: buffer mapping, WAL
    insert, buffer content locks, etc.)
  - bufmgr.c:6190 (buffer pin wait)
  - clog.c:547 (XACT_GROUP_UPDATE), procarray.c:815 (PROCARRAY_GROUP_UPDATE)

Unlike WaitLatch/WaitEventSet waits (already fiber-yielding via xtc_pg_wait_fd),
a fiber that blocks here stalls its ENTIRE carrier loop (all sibling fibers on
that OS thread) until ProcWakeSemaphore -> PGSemaphoreUnlock (sem_post).  This
is invisible on the cached read-only workload (LWLock waits rare -> parity
achieved in Phase 18), but on a WRITE/contended workload (WAL insert, buffer
content locks, XactLockTable) it is expected to be the dominant threaded
throughput-vs-fork gap.

Phase 17 target (concrete): make ProcWaitOnSemaphore fiber-aware.  Instead of a
raw carrier-blocking sem_wait, the waiting fiber yields to its loop and is woken
by ProcWakeSemaphore.  Note ProcWakeSemaphore already has the wait-completion /
PgBackendWakeWaitCompletionById hook and could drive an xtc_proc_wake of the
target fiber -- so the wake side is half-built.  The risk is the wake RACE
(waiter must arm its fiber-wake and re-check the semaphore/condition atomically,
or a wake between arm and park is lost -> hang) and the fact that the waiter
holds no LWLock at this point (good) but IS mid-command with a live fiber stack
(fine -- stackful).

NEXT SESSION plan:
1. Measure first (re-scoping discipline): a WRITE/contended pgbench (TPC-B or a
   hot-row UPDATE) at N carriers < clients -- does throughput collapse vs process
   because carriers block in sem_wait?  This confirms the fix is worth the risk.
   (Cached -S showed nothing; the contended case is where it bites.)
2. If confirmed, design the fiber-aware wait: a per-PGPROC fiber-wake handle
   (loop_id/local_id, like the latch owner_fiber capture in waiteventset.c) armed
   before the "still need to wait?" re-check, parked via xtc_proc_wait (fd-less /
   waker), woken by ProcWakeSemaphore -> xtc_proc_wake.  Keep the raw
   PGSemaphoreLock fallback for the process/non-fiber path and as the safe
   default when the fiber-wake is not armable.  A/B on check-threaded-pooled +
   the contended workload; must be neutral-or-better and pass a
   cancellation/timeout/wake-race stress test before landing.

### Phase 17 design finding: the fd-less park gap + two implementation options (2026-07-14)

Traced the full wait path for the fix design (no code yet, per measure-first):

- PgSuspend (backend_runtime_backend.c) is ONLY wait-observability: it publishes
  the wait-completion diagnostic, then calls the callback.  For the semaphore
  path the callback is ProcSemaphoreWaitCallback -> PGSemaphoreLock -> raw
  sem_wait (posix_sema.c:313).  PgSuspend does NOT yield the fiber.  Confirmed:
  ProcWaitOnSemaphore blocks the carrier.
- Wake side is half-built: ProcWakeSemaphore already calls
  PgBackendWakeWaitCompletionById (diagnostic) and PGSemaphoreUnlock; it is the
  natural place to also fire an xtc_proc_wake of the waiter's fiber.
- Pattern to mirror: the latch path (waiteventset.c:1393) captures the waiter's
  xtc_self() as owner_fiber_{loop,local,gen} on the Latch at the park point, and
  the cross-thread waker reconstructs the xtc_pid_t and calls xtc_proc_wake.
  The semaphore fix needs the same handle on PGPROC.

GAP found: there is no clean "park indefinitely until xtc_proc_wake" today.
xtc_pg_wait_fd with fd<0 and an infinite timeout just returns WL_LATCH_SET
immediately (does NOT park) -- see pg_xtc_carrier.c:797-808.  So a fiber-aware
semaphore wait needs one of:

  Option A (xtc_proc_wake-park): waiter arms its fiber pid on PGPROC, parks in an
    fd-less wait (xtc_recv on a private channel, or a new fd-less parkable
    primitive), ProcWakeSemaphore -> xtc_proc_wake.  Needs a genuine fd-less
    park; xtc_recv works but couples to the mailbox.
  Option B (eventfd-per-PGPROC) -- LOWER RISK, PREFERRED to try first: give each
    PGPROC an eventfd; the fiber waiter parks via the PROVEN xtc_pg_wait_fd(fd)
    path (same machinery as the pooled-queue eventfd fix, e0880ddb823-era);
    ProcWakeSemaphore writes the eventfd (and still sem_post for the process/
    non-fiber path).  Reuses working fd-park code, no new libxtc primitive, and
    the wake-race is handled the same way the eventfd already is (write-then-
    reader-drains; a wake before park leaves the fd readable so park returns
    immediately -- no lost wake).

Either way the arm/re-check/park order must be: arm fiber-wake handle -> re-check
the wait condition (semaphore trylock / the actual predicate) -> park only if
still-must-wait, so a ProcWakeSemaphore between arm and park is not lost.

Still MEASURE FIRST next session (contended-write pgbench, carriers<clients) to
confirm the carrier-starvation is real before landing an eventfd-per-PGPROC or
fiber-wake change to this hot path.  If real: implement Option B, A/B on
check-threaded-pooled + the contended workload, wake-race/cancel/timeout stress
test, keep raw sem_wait fallback.

### Phase 17: NO libxtc gap after all -- xtc_sem / xtc_notify already exist (2026-07-14)

Validated the "fd-less park" gap before writing a libxtc request (per
verify-before-requesting).  The gap does NOT exist: libxtc v1.21.0 already ships
in xtc_sync.h exactly the primitives needed --

  - xtc_notify (xtc_notify_signal / xtc_notify_wait): "Block (yield) the calling
    task until a signal arrives" -- the fd-less park-until-wake I had thought was
    missing.  Stored-signal semantics (a signal before wait returns immediately)
    => no lost-wake race by construction.
  - xtc_sem (xtc_sem_create/post/acquire/try_acquire): a FIBER-AWARE counting
    semaphore whose impl explicitly "park(s) (yield to the loop)... never
    blocking the OS thread" for fiber waiters, with a raw-thread fallback.  This
    is a direct semantic match for PGSemaphore.

So NO libxtc report is warranted (and I did not file one).  The earlier
xtc_pg_wait_fd(fd<0) dead-end was a limitation of ONE primitive, not of libxtc.
Corrected conclusion: the Phase 17 fix is entirely PG-side -- back the
ProcWaitOnSemaphore path (LWLock/buffer-lock/group-update deep waits) with
xtc_sem or an xtc_notify-per-PGPROC when in a backend fiber, instead of the raw
carrier-blocking sem_wait.  Prefer this over the eventfd-per-PGPROC Option B: no
per-PGPROC fd cost, xtc_notify's stored-signal semantics remove the arm/park
wake-race, and it uses libxtc's own fiber-aware wait (the fusion direction the
north star wants).  Keep raw sem_wait for the process / non-fiber path.

(Lesson reinforced: checked xtc_sync.h before requesting -- the primitive was
already there.  This is the third time measurement/verification changed the
answer; keep doing it before any libxtc ask.)

### Phase 17 measurement: BLOCKED on local SSH-MTU issue (2026-07-14)

The libxtc question is CLOSED (no report -- xtc_sem/xtc_notify already exist; see
above).  The measurement to ungate the fix (confirm carrier-starvation on a
contended-write workload) is set up but blocked on an environment issue:

- Fresh EC2 m6id.8xlarge boxes are unreachable via SSH: TCP connects to :22 but
  the SSH key exchange HANGS (stalls right after selecting curve25519-sha256).
  Root cause: the local machine has a 1280-MTU interface (VPN/WireGuard tunnel);
  the large KEX packet is blackholed (PMTU discovery failing across the tunnel).
  Reproduced on two boxes in different subnets; not instance-specific.
- us-east-1a currently has NO m6id.8xlarge capacity (use 1b/1c/1d/1f/1h or omit
  AZ).

Reusable AWS state left in place (cheap): key-pair `xtc-p17` (~/.ssh/xtc-p17.pem),
SG `sg-0b26d93900cc44e16` (VPC vpc-073b7edea5b4f3931, port 22 from the launcher
IP), env in /tmp/xtc_p17_aws.env.  NO instances left running (both attempts
terminated / never launched).

NEXT SESSION -- fix SSH first, then measure:
  * SSH-MTU workaround options: (a) on the launcher, clamp outbound MSS to the
    1280 tunnel: `sudo iptables -t mangle -A OUTPUT -p tcp --tcp-flags SYN,RST
    SYN -j TCPMSS --clamp-mss-to-pmtu` (or --set-mss 1240); (b) or add
    user-data at launch to set the instance eth0 MTU to 1400 and enable MSS
    clamping so its KEX replies fit; (c) or SSH via a jump box on a 1500-MTU
    path.  (a) is the least invasive.
  * Then: pgbench TPC-B or a hot-row UPDATE at carriers < clients (e.g. 8
    carriers, 32-64 clients), threaded vs process, watch for a throughput
    collapse caused by carriers blocking in sem_wait (ProcWaitOnSemaphore).
    Cross-check with a perf/off-CPU trace showing time in sem_wait /
    PGSemaphoreLock on the LWLock acquire slow path.  If the collapse is real,
    implement the xtc_sem/xtc_notify fix (PG-side, per the design note above).

### Phase 17: SSH blocker was MISDIAGNOSED -- it was agent key-flooding, not MTU (2026-07-15)

Last session I attributed the fresh-EC2 SSH failure to a 1280-MTU tunnel
blackholing the KEX packet.  That was WRONG (attributed without capturing a
verbose auth trace).  Re-measured this session:
  - The route to the EC2 public IP egresses on wifi (MTU 1500), not tailscale.
  - A 5MB HTTPS download works fine -> large-packet TCP is healthy locally.
  - SSH KEX COMPLETES (NEWKEYS both ways, SERVICE_ACCEPT); the hang is at AUTH.
  - `ssh -vvv` shows the local ssh-agent offering ~6 keys; each gets type 51
    (rejected); after MaxAuthTries the server sends type 1 (disconnect).  The
    `-i xtc-p17.pem` key never gets prioritized because the agent floods first.
FIX: `ssh -o IdentitiesOnly=yes -o IdentityAgent=none -i <key>` (use ONLY the
named key, ignore the flooded agent).  MTU/user-data/MSS experiments were all
red herrings.  Recorded in /tmp/xtc_p17_aws.env as SSHOPTS.
(Lesson: measure the actual failure -- the -vvv auth trace -- before naming a
cause.  The MTU story was plausible and completely wrong.)


### Rebase 2026-07-16: two regressions to root-cause before force-push (test-world NOT green)

Rebased xtc onto origin/master (0 behind, 106 ahead, no conflicts).  Build is
warning-clean (0/0) and process regress = 245/245.  Held the force-push: two
threaded-suite regressions surfaced (both need root-cause; NOT stale tests):

1. test_backend_runtime/regress -- "closed backend runtime state was not reset:
   retained_top=X expected_top=X proc_exit_done=1".  retained_top==expected_top
   and proc_exit_done=1 are CORRECT, so the failing clause is one of the
   utility->* cache checks (dch_counter / num_cache / format_cache_context /
   libxml_context / missing_attr_cache) in test_backend_runtime_backend.c:1558+.
   Our reset (backend_runtime_teardown.c:708-720) does clear them, so this is a
   subtle reset-completeness or MALLOC_PERTURB-exposed ordering issue after the
   rebase.  Root-cause: instrument which && clause is false.

2. 001_threaded_runtime test 52 "process-only module rejection reports backend
   model mismatch": the rejection MESSAGE is correct ("active backend model is
   thread-per-session, but the library is process-only") but the backend fiber
   then SIGSEGVs -- "xtc: SUPERVISOR observed GENUINE-CRASH backend fiber DOWN
   signal=11" -> "terminating threaded server runtime after backend fiber
   crash".  So the Phase 19 process-only-extension rejection path crashes on the
   error unwind under threaded mode (was clean before the rebase).  Likely the
   rebase changed dfmgr/extension-load or an error-path teardown the threaded
   fiber unwind touches.  Root-cause via post-mortem core (gdb attach blocked).

FIXED this rebase (correct, committed): 18->0 build warnings; 001's stale
io-worker expectation (now asserts ZERO io workers, matching c093c214cf4).
Backup: xtc-pre-rebase-202607160519.  DO NOT force-push origin/xtc until both
regressions are fixed and check + check-threaded are green.

### Rebase 2026-07-16 UPDATE: 2 of 3 regressions fixed; #2b (bgworker-launch SIGSEGV) open

Fixed + committed (a03379cda28, d9c7b27447c): all 18 build warnings (0/0 now);
regress reset-check (num_external_fds is preserved-by-design, test corrected);
the _PG_init LWLockNewTrancheId preload SIGSEGV (upstream made it shmem/spinlock-
based -> can't call pre-shmem; deferred to lazy first-use); stale 001 io-worker
and process-only-rejection-regex expectations.  Process regress 245/245;
test_backend_runtime 12/14; build warning-clean.

STILL OPEN -- regression 2b: 001_threaded_runtime SIGSEGVs at line 628
(test_backend_runtime_launch_thread_bgworker) but ONLY after the full ~627-line
preamble; NOT reproducible in isolation (a fresh threaded server launches the
thread bgworker and returns a pid cleanly, even after running CV/lwlock/parallel
tests manually).  So it is STATE/ORDERING-dependent.  Could not get a core (the
meson-test env + carrier crash handler suppress core dumps).  003_milestone also
ERRORs (likely same root cause -- it's a smoke over the same paths).

NEXT SESSION (turnkey): capture the crash core.  Options: (a) run 001 via
`meson test` but wrap postgres so PG_XTC_ALLOW_CORE=1 + ulimit -c unlimited +
kernel.core_pattern survive into the temp-instance backend (the meson testwrap
resets them -- set them in the node's postgresql.conf / a PG_TEST_INITDB or via
a postmaster wrapper script referenced by the node), or (b) bisect 001: binary-
search which preamble test, when run before the launch, triggers the crash (add
an early `done_testing` / splice the .pl), then reproduce just that pair in the
core-capturing harness (/tmp/bgw2.sh pattern).  Prime suspects for the state:
WaitEventExtensionNew/WaitEventCustomNew (shmem+LWLock, called at threaded.c:589/
621/668 across fibers -- custom-wait-event shmem array exhaustion/corruption
across carriers?), or the parallel-query (debug_parallel_query=on) test at
001:~650 leaving parallel/DSM state that the subsequent bgworker fiber launch
dereferences.  Backtrace will name it.  DO NOT force-push origin/xtc until 001+
003 are green and check+check-threaded pass.  Backup: xtc-pre-rebase-202607160519.

### Regression 2b -- SHARPENED diagnosis (2026-07-16, session 2)

Could NOT capture a core: the carrier fault path (libxtc fault guard) restores
SIG_DFL and re-raises, but no core lands even with PG_XTC_ALLOW_CORE=1 + ulimit
-c unlimited + kernel.core_pattern=/tmp/... (the re-raise on a fiber/alt stack
does not dump; the meson/TAP node also tears its data dir down on END).  So I
diagnosed by elog(LOG) markers instead.

KEY NEW FINDINGS (narrows it a lot):
- The SIGSEGV is in the CLIENT BACKEND FIBER that runs
  test_backend_runtime_launch_thread_bgworker (pid loop=2,local=2,GEN=7), NOT in
  the bgworker main.  Confirmed: a marker at the very FIRST line of the launch
  function ("XTCDIAG launch: function entered") did NOT print before the crash
  (in the full-001 run), while the bgworker-main markers never print either ->
  crash is at/just-before the launch function prologue.
- In ISOLATION the launch + bgworker-main run cleanly every time (marker sequence
  complete, returns a pid), even after manually running CV/lwlock tests and the
  reject-process-bgworker -> launch pair.  So it is STATE/ORDERING-dependent on
  001's FULL ~627-line preamble (plpgsql/plsample/plperl + custom-GUC LOAD stress
  + a 4-worker GUC-stress loop + parallel-query w/ debug_parallel_query=on).
- gen=7 = a heavily-REUSED fiber slot on loop 2.  Combined with "crashes at the
  launch prologue only after heavy prior load", the leading hypotheses are now:
  (a) per-fiber/per-backend runtime state not fully reset across fiber-slot
      REUSE (gen bump) -- the launch prologue touches PgCurrentBackendSignalPid()
      and other per-backend accessors; a stale/unmapped bucket for a reused slot
      would SIGSEGV before the function body; or
  (b) FIBER STACK exhaustion/overflow -- plperl + parallel-query recurse deeply;
      if a prior deep operation on this reused fiber left the 8MB stack near its
      guard, the next call faults.  (Note: gen=7 reuse + deep prior frames.)
  DISTINGUISH: (a) instrument PgRuntime per-backend bucket validity at launch
  entry for a reused (gen>1) fiber; (b) bump XTC_PG_FIBER_STACK and see if the
  crash moves/vanishes, or add a stack-depth log at launch entry.

TURNKEY REPRO (fast, no meson): the crash needs the full preamble, so BISECT --
copy 001.pl, insert `done_testing(); exit;` after progressively-earlier preamble
blocks (parallel-query block ~650; the 4-worker GUC loop; the LOAD-stress ~500s;
plperl), rebuild nothing (it's the .pl), run via the direct-perl harness used
this session, and find the minimal preamble prefix that still crashes the launch.
Then reproduce that prefix + launch in /tmp/bgw4.sh (which captures state on a
core-friendly direct-start server) to get the backtrace.  Build is warning-clean;
regress 245/245; 12/14 test_backend_runtime; force-push HELD until 001+003 green.

### Regression 2b -- further narrowed (2026-07-16 session 3): NOT stack, NOT simple reuse

Answering the standing questions + two distinguishing tests run:

Q: Is plperl running in a thread in the main process?  A: YES under
multithreaded=on -- plperl is PG_MODULE_MAGIC_BACKEND_MODEL_POOLED_PROTOCOL_AFFINE,
so the embedded Perl interpreter runs in the backend FIBER on a carrier OS thread,
in the single postmaster address space.  BUT 001 test 628 runs under
pooled_protocol_carriers=0 (THREAD-PER-SESSION), where plperl's my_perl-drift
fast-path is skipped and each session is its own fiber -- so the shared-my_perl
hazard is not in play on this test.

Q: Is that level of recursion normal / is it a stack problem?  A: NO -- FALSIFIED.
Bumped XTC_PG_FIBER_STACK 8MB -> 32MB, rebuilt, re-ran 001: STILL crashes at test
56/line 628 (exit 29).  So the launch SIGSEGV is NOT fiber-stack exhaustion.
Also: each safe_psql in the preamble is a FRESH connection/fiber, so no single
deep plperl/parallel recursion is in-flight at the crash; the crashing fiber
(gen=7) started clean.

Also FALSIFIED this session: simple fiber-reuse count is not the trigger (20
plperl-function fiber reuses then launch = clean, returns a pid); and
plperl-then-launch is not the trigger.

So 2b is a SPECIFIC preamble-content ordering dependency, in the launch fiber's
prologue, that survives across the reused slot.  How to address / next:
  - BISECT 001.pl (the remaining turnkey step): copy 001, insert
    `done_testing(); exit(0);` progressively earlier in the preamble, find the
    minimal set of preceding blocks that still crashes the launch.  Prime
    remaining suspects (not yet excluded): the parallel-query block
    (debug_parallel_query=on, ~line 650 -- leaves parallel/DSM/worker state), the
    custom-GUC LOAD-stress + 4-worker GUC loop (~500-560, repeated
    LOAD 'test_backend_runtime_threaded' + SET across sessions), or plsample.
  - Then reproduce the minimal prefix + launch in /tmp/reuse.sh-style harness on a
    direct-start server (core-friendly) to get a backtrace, OR add an elog marker
    at each RegisterDynamicBackgroundWorker/BackgroundWorkerData access to see the
    exact crashing access.
  - Likely area given "launch prologue on a reused fiber after specific prior
    ops": BackgroundWorkerData / bgworker slot shmem state, or a per-backend
    runtime bucket left dangling by the parallel-query or GUC-worker path when its
    fiber slot is later reused by the launch caller.

STATE: 2 of 3 rebase regressions FIXED (regress reset-check; _PG_init preload
SIGSEGV) + 0 build warnings + stale-test fixes, all committed.  2b open, sharply
narrowed (not stack, not simple reuse).  Force-push HELD; benchmark NOT run
(won't benchmark with a live threaded crash).  Backup: xtc-pre-rebase-202607160519.

### Regression 2b ROOT-CAUSED (2026-07-16 session 4): ThreadedGUCMutex deadlocks the carrier

BREAKTHROUGH.  Reduced 2b to a MINIMAL repro (no bgworker): under multithreaded=on
(thread-per-session, carriers=0, but the runtime still uses the N-loop executor),
several CONCURRENT sessions each doing `LOAD 'test_backend_runtime_threaded'` +
a tight `set_config('...custom_guc', ...)` loop -> the server HANGS (earlier runs
reported "crash"; the careful liveness check shows the postmaster hangs, wait
never returns, exit=124).  The kernel segfaults seen earlier were the OLD
_PG_init/LWLockNewTrancheId crash + a stale-.so artifact; the TRUE 2b symptom is a
HANG/DEADLOCK, not a segfault.

ROOT CAUSE: guc.c ThreadedGUCLock() does pthread_mutex_lock(&ThreadedGUCMutex) --
a PROCESS-GLOBAL raw pthread mutex (guc.c:107) -- around GUC startup/SET/RESET.
This is the SAME bug class as Phase 17's sem_wait: a raw pthread_mutex_lock BLOCKS
THE CARRIER OS THREAD, not the fiber.  If a fiber takes ThreadedGUCMutex and then
YIELDS the carrier at any point while holding it (a memory alloc that parks, an
internal wait, or simply the scheduler switching fibers on that loop), another
fiber scheduled on the SAME carrier thread that also calls ThreadedGUCLock ->
pthread_mutex_lock blocks the whole OS thread -> the holder can never be
rescheduled on it -> deadlock.  HOLD_INTERRUPTS() only defers PG interrupt yields,
not fiber scheduler yields.  Concurrent custom-GUC set across fibers hits this
reliably (custom-GUC assign does guc_malloc/guc_strdup + hash work under the lock).

FIX DIRECTION (next session -- the real 2b fix):
  - The GUC critical section must be a FIBER-AWARE lock, not a raw
    pthread_mutex_lock that blocks the carrier.  Options: (a) make ThreadedGUCLock
    use a libxtc fiber-aware mutex (xtc_amutex / xtc_sem) so a waiter YIELDS the
    carrier instead of blocking it (mirrors the Phase 17 direction); (b) guarantee
    NO yield happens while the mutex is held (audit the SET/RESET/startup path
    under the lock for any alloc/wait that can park; if none, the only remaining
    risk is the scheduler preempting the fiber mid-hold -- so the lock must be
    non-preemptible or fiber-aware anyway); (c) make GUC state fully per-backend
    so no cross-fiber mutex is needed for SET/RESET (the table already is
    per-backend via PgCurrentGUCVariablesRef -- audit WHY a process-global mutex
    is still taken; if it only guards the shared custom-GUC-registration/prefix
    reservation, narrow it to just that and drop it from the hot SET/RESET path).
  - Prefer (a) or (c).  A/B + check-threaded after.  This is a CORE threaded-
    runtime bug (any concurrent custom-GUC or GUC-heavy threaded workload hits
    it), higher priority than a test artifact.

STATE: regression 1 (reset-check) + 2a (_PG_init preload SIGSEGV) FIXED; the
custom-GUC-per-backend pattern fix committed (69a67457711, correct but not the
whole story); 2b now ROOT-CAUSED to ThreadedGUCMutex carrier-blocking.  0 build
warnings.  Force-push HELD; benchmark NOT run.  Backup: xtc-pre-rebase-202607160519.

### Regression 2b UPDATE (session 5): amutex fix landed -- deadlock GONE, residual SIGSEGV remains

FIXED (committed a8d4a4440c0): the ThreadedGUCMutex deadlock, via a fiber-aware
xtc_amutex (xtc_amutex_static slot 0) replacing the raw pthread_mutex_lock in
guc.c ThreadedGUCLock/Unlock under USE_XTC_CARRIER.  Verified: 1 and 2 concurrent
sessions doing LOAD + set_config('test_backend_runtime_threaded.custom_guc') loops
now COMPLETE cleanly (were hanging).  Deadlock resolved.

STILL OPEN: 001 STILL SIGSEGVs (signal=11, loop=2/local=2/gen=7, exit 29 after
test 56) under its FULLER concurrency (the 4-worker GUC-stress loop, each worker a
persistent psql doing 25x set_config on the custom GUC + built-in GUCs
concurrently).  This is a SEPARATE crash from the deadlock the amutex fixed --
the amutex serialization is correct, but something in the concurrent custom-GUC
path still faults.  My ad-hoc /tmp harnesses gave unreliable logs (empty/cleaned);
2-worker completes but the server appeared to die on the follow-up liveness check
(inconclusive -- harness noise).

NEXT SESSION (turnkey):
  - Get a real backtrace of the residual SIGSEGV.  Best route: run 001 but make
    the temp node keep cores -- the reliable way is to add
    `$node->append_conf('postgresql.conf', ...)` is not enough (RLIMIT_CORE);
    instead run postgres under a wrapper that does `ulimit -c unlimited` +
    PG_XTC_ALLOW_CORE=1 and points core_pattern to an ABSOLUTE writable path, OR
    reproduce with the /tmp/gv3.sh-style harness scaled to 4 workers WITH
    per-worker `timeout` + copy the server log BEFORE teardown.  (Note libxtc's
    fault guard suppresses cores unless PG_XTC_ALLOW_CORE=1; even then the
    re-raise on a fiber stack may not dump -- if so, use elog markers around the
    custom-GUC assign_hook / string-free and the per-backend
    PgCurrentTestBackendRuntimeCustomGucRef path to bisect the faulting access.)
  - SUSPECTS for the residual crash (concurrent custom-GUC, amutex now serializes
    the critical section so it is NOT a lost-update race on the table): the
    per-backend custom-GUC STRING free/realloc across fiber generations (the
    value points into a per-session/GUC memory context that may be reset while
    another fiber references it), or guc report/nondef/stack list state, or the
    module's own custom-GUC storage lifetime vs the per-session extension-module
    state reset.  Also re-check: does the amutex actually cover the custom-GUC
    ASSIGN (string dup/free), or only registration/startup?  If assign runs
    OUTSIDE the critical section, concurrent assigns to the same per-backend...
    (no -- per-backend, so not shared) -- re-verify the crash is truly
    concurrency-triggered vs. a custom-GUC-string-context-lifetime bug that a
    single session's repeated set_config + session reset also hits.
  - Then confirm 001 + 003 green, run gmake check + check-threaded, THEN
    force-push origin/xtc and run the benchmark.

STATE: regressions 1 + 2a FIXED; 2b deadlock FIXED (amutex); residual 2b SIGSEGV
open.  0 build warnings; process regress 245/245.  Force-push HELD; benchmark NOT
run.  Backup: xtc-pre-rebase-202607160519.

### Regression 2b residual SIGSEGV -- further narrowed (session 6)

Did NOT force-push (001/003 still SIGSEGV; will not ship/benchmark a crashing
branch even with permission -- the gate is test-world green).

Narrowed the residual concurrent-custom-GUC crash:
- FALSIFIED single-session/context-lifetime: 30 SEQUENTIAL sessions each doing
  LOAD + set_config('...custom_guc') = NO crash, server alive.  So it is
  genuinely CONCURRENCY-triggered, not a per-session reset/context-lifetime bug.
- CONFIRMED the custom-GUC assign IS serialized by the (now fiber-aware) amutex:
  set_config_with_handle takes ThreadedGUCLock unless
  GUCSetOptionNeedsThreadedLock()==false, and that returns TRUE for
  custom/extension records (GUCRecordIsCurrentSessionBuiltin is false for a
  custom GUC -> the "skip lock" && chain is false -> needs lock).  So concurrent
  custom-GUC set_config DOES hold the amutex.

So the residual crash is NOT an unserialized-assign race and NOT single-session.
It is concurrency-triggered but the assign critical section is serialized ->
the fault is likely in state touched OUTSIDE the critical section but still
shared/aliased across fibers, e.g.:
  - the custom-GUC STRING value: allocated in a per-session GUC memory context,
    pointer stored in PgCurrentSessionExtensionModuleState() (my per-backend
    fix).  Check whether the GUC value string and the module's per-session
    extension-state have MISMATCHED lifetimes/contexts across fibers, or whether
    the value string context is shared where it should be per-session.
  - GUC report/nondef/stack LIST state (guc_report_list / guc_nondef_list /
    guc_stack_list) -- per-backend accessors, but verify they are truly
    per-session and not aliased on a shared carrier.
  - the placeholder->custom reclassification when LOAD defines the GUC while a
    concurrent session already set the placeholder.

NEXT: get the backtrace.  Since libxtc suppresses cores, the reliable path is
elog markers: instrument the custom-GUC string set/free (set_string_field /
guc_strdup / the assign of GUC_VARIABLE_STRING) and the placeholder-conversion
path, run 001 (or a 4-worker /tmp harness with per-worker timeout + pre-teardown
log copy), and find the faulting access.  Then fix, confirm 001+003 green +
gmake check/check-threaded, THEN force-push + benchmark.

STATE unchanged otherwise: regressions 1 + 2a fixed; 2b deadlock fixed (amutex,
a8d4a4440c0); residual 2b SIGSEGV open + narrowed.  0 warnings; regress 245/245.
Backup: xtc-pre-rebase-202607160519.

### Regression 2b residual SIGSEGV -- session 7: custom-GUC assign is CLEAN; crash is a later op

Instrumented the custom-GUC string assign (set_config_with_handle_internal, the
set_string_field for test_backend_runtime_threaded.custom_guc) with an elog marker
logging field_ptr / session_owned / newval, ran the 4-worker GUC stress AND 001.

FINDINGS:
- FALSIFIED the custom-GUC-aliasing hypothesis: every custom_guc assign shows a
  DISTINCT per-session field_ptr (one per session's extension-module-state) and
  session_owned=1.  My per-backend fix (69a67457711) is correct -- no shared/
  stale address.  (Aside: newval is sometimes a stack addr 0x7fff... at the log
  point, but that is BEFORE set_string_field guc_strdup's it into a stable
  context, so benign.)
- A standalone 4-worker/25-iter custom_guc stress harness now PASSES (4/4 fibers
  exit code=0, no crash) with the amutex fix -- so pure concurrent custom_guc set
  is FIXED.
- BUT 001 STILL crashes (signal=11, loop=2/local=2/gen=7, exit 29 after test 56).
  Timeline: last custom_guc assign at T; crash ~57ms LATER on a DIFFERENT fiber
  (gen=7).  So the crash is NOT the custom-GUC assign -- it is a LATER operation
  after the full GUC-stress preamble (the process-only-rejection / thread-bgworker
  launch region, tests 51-56+).  libxtc's fault guard catches the SIGSEGV before
  the kernel logs it (no dmesg entry), so no core/kernel addr.

REMAINING HYPOTHESIS (next): the crash is triggered by CUMULATIVE GUC-stress state
(001 stresses 8 GUCs incl. custom_guc x50 across 4 concurrent workers) then a
later op faults.  DISTINGUISHING TEST (turnkey): edit 001's GUC-stress loop to use
ONLY built-in GUCs (drop the custom_guc set_config) -> if 001 still crashes, the
trigger is general GUC-stress/session-reset, not custom-GUC; if it passes, custom-
GUC-stress leaves bad state a later op hits.  ALSO: instrument the thread-bgworker
launch prologue + the process-only-rejection path (tests 51-56) with elog markers
to find the faulting op, OR bisect 001 by inserting done_testing();exit; after the
GUC-stress block (before the reject/launch) to confirm the GUC-stress alone is
survivable and the crash is in the reject/launch region.

STATE: regressions 1 + 2a fixed; 2b deadlock fixed (amutex a8d4a4440c0); pure
concurrent custom-GUC set fixed; residual 001 crash is a LATER op after GUC
stress (custom-GUC assign ruled out).  0 warnings; regress 245/245.  Force-push
HELD; benchmark NOT run.  Backup: xtc-pre-rebase-202607160519.

### Regression 2b -- session 8: PINPOINTED to launch_thread_bgworker after GUC-stress

Bisected 001 with done_testing();exit; markers (no source changes to 001 itself;
used /tmp/001_bisect*.pl copies):
- GUC-stress block ALONE (tests 1-50): PASSES cleanly, exit 0.  Survivable.
- + process-only LOAD reject + process-bgworker reject (tests 51-56): PASS
  cleanly.  Survivable.
- The crash is the VERY NEXT call: test_backend_runtime_launch_thread_bgworker()
  (thread-model bgworker launch).
- reject_process_bgworker() -> launch_thread_bgworker() IN ISOLATION (no
  GUC-stress): WORKS (reject=t, launch=pid, ALIVE).  So the reject does not
  corrupt the launch by itself.
=> CONFIRMED TRIGGER: the 4-concurrent-worker GUC-STRESS preamble leaves state
that makes a LATER thread-bgworker LAUNCH crash (SIGSEGV, gen=7 fiber).  Not the
custom-GUC assign (session 7), not the reject path, not the launch in isolation.

MINIMAL REPRO to build next (turnkey): on a threaded server, run the 4-concurrent
GUC-stress (LOAD test_backend_runtime_threaded + 25-50x set_config over the 8
GUCs incl custom_guc, from 4 concurrent sessions), let them finish, THEN
SELECT test_backend_runtime_launch_thread_bgworker() -> expect SIGSEGV.  (The
/tmp/gucstress.sh + a trailing launch approximates this; scale to match 001:
work_mem/default_statistics_target/lock_timeout/search_path/bytea_output/
IntervalStyle/wal_consistency_checking + custom_guc.)  Then instrument the LAUNCH
path (RegisterDynamicBackgroundWorker / BackgroundWorkerData slot access /
WaitForBackgroundWorkerStartup) with elog markers to find the faulting access on
the gen=7 fiber, OR bisect WHICH of the 8 stressed GUCs leaves the bad state
(drop custom_guc first, then the reloadable ones like wal_consistency_checking).
Suspect: a GUC-stress side effect on shared bgworker/parallel/DSM registration
state, or a per-backend runtime bucket left dangling that the launch prologue
reads.

STATE: regressions 1 + 2a fixed; 2b deadlock fixed (amutex a8d4a4440c0); pure
concurrent custom-GUC fixed; residual crash PINPOINTED to launch-after-GUC-stress.
0 warnings; regress 245/245.  Force-push HELD; benchmark NOT run.  Backup:
xtc-pre-rebase-202607160519.

### Regression 2b -- session 9: crash localized to fiber WaitLatch/WaitEventSet path (bgworker launch region), NOT custom-GUC

Major diagnostic progress via elog milestone markers in 001 (the ground-truth
repro; /tmp harnesses could NOT reproduce -- see below).

CHAIN (all via markers, then reverted -- tree clean):
- The gen=7 signal=11 crash after test 56 is a bgworker fiber that reaches
  BackgroundWorkerMain milestone m5 (pre-entrypt) then faults IN entrypt().
- entrypt for the crashing fiber was fn=ApplyLauncherMain (the logical-rep
  launcher, a BgWorkerBackendThreadPerSession bgworker) -- reaches its main
  loop, survives get_subscription_list, and faults INSIDE WaitLatch ->
  WaitEventSetWait -> WaitEventSetWaitBlock (the xtc fiber intercept:
  xtc_pg_wait_fd on set->epoll_fd).  Matches an earlier bgwriter core that
  faulted at waiteventset.c:1311 (set->latch->maybe_sleeping) with a corrupted
  set, and a spi_printtup PANIC (_SPI_current==NULL) core.
- FALSIFIED "apply launcher is the culprit": max_logical_replication_workers=0
  -> 001 STILL crashes (now a GUC-stress worker fiber, gen=7).  So it is a
  GENERAL fiber/concurrency corruption at the GUC-stress + thread-bgworker-launch
  sequence, crashing whichever fiber -- not one specific worker.
- Common denominator of every signature: per-backend state resolved through
  __thread current-work pointers (CurrentPgBackend/CurrentPgExecution) --
  LatchWaitSet (PgCurrentBackendIPCState), SPI _SPI_current (PgExecution).  One
  marker run caught a fiber logging as "unrecognized[0]" (MyProcPid==0) sharing
  another backend's LatchWaitSet (same epoll_fd + same latch ptr as a valid
  backend) -- i.e. a fiber with lost/stale backend identity resolving a foreign
  per-backend WaitEventSet.  Strongly implicates __thread current-work-state
  coherence across fiber scheduling.

CORRECTIONS to earlier sessions:
- The earlier "single-carrier clean / multi-carrier ~30% crash" result was
  CONTAMINATED by leftover stray postgres servers from prior harness runs
  (3+ clusters running at once -> port/socket/resource collisions).  After
  killing strays, the /tmp harness (even matching 001's config: autovac,
  io_method=worker, wal_consistency_checking, BEGIN/COMMIT, 4 workers) runs
  8/8 CLEAN and never migrates (XTCMIG tid-change detector = 0).  So the crash
  is NOT reproduced by fire-and-forget concurrent GUC-stress.
- libxtc pins xtc_proc_* fibers (xtc_async passes pinned=1 to
  __xtc_task_spawn_ex; loop_int.h: pinned tasks never work-stolen).  So PG
  backend/worker fibers do NOT migrate loops.  Migration is NOT the cause.
- pooled_protocol_carriers=1 on 001 fails EARLIER (after test 10, no
  GENUINE-CRASH) -- under-provisioned (001 needs several carriers for its
  concurrent sessions); a red herring, not the same crash.

WHY /tmp can't reproduce but 001 does: 001 uses background_psql + pump_until to
run the 4 GUC-stress workers INTERLEAVED with the perl driver, finishing them
one-at-a-time IN ORDER (per-worker $psql->{run}->finish), THEN does the
thread-bgworker launch.  That precise open-session + ordered-teardown + launch
timing is the trigger; fire-and-forget psql does not match it.

BLOCKER on the exact faulting line: the crash is signal=11 (SIGSEGV) caught by
libxtc's fault guard, which re-raises on the fiber/alt stack and does NOT dump a
core even with PG_XTC_ALLOW_CORE=1 + ulimit -c unlimited (confirmed again this
session, incl. from 001's own postmaster).  Only the rarer spi_printtup PANIC
(SIGABRT) dumps.  elog markers work but shift timing (Heisenbug -- the crashing
fiber identity/line moves when markers are added).

NEXT (turnkey): (a) reproduce in a harness that mirrors 001's background_psql
ordered-teardown-then-launch timing (persistent psql sessions via coproc/expect,
finished in order, then launch) so a core lands; OR (b) instrument the fiber
adopt/reset/current-work path (PgCarrierAttachBackend / PgRuntimeSetCurrentWork /
the read-command park/resume) to catch a fiber running with MyProcPid==0 or a
mismatched CurrentPgBackend during a WaitEventSetWaitBlock, which the
"unrecognized[0] shares foreign LatchWaitSet" marker already hinted at; OR (c)
add a cheap assertion (MyProcPid!=0 && set owner == current backend) at the
WaitEventSetWaitBlock xtc-branch entry and run 001 to fail-fast at the corruption
point rather than the downstream deref.  The fix is almost certainly restoring/
validating __thread current-work (CurrentPgBackend/Execution) coherence at the
fiber resume boundary for aux-worker/bgworker fibers.

STATE: regressions 1 + 2a fixed; 2b GUC deadlock fixed (amutex a8d4a4440c0);
residual 001 crash is a fiber WaitLatch/WaitEventSet per-backend-state corruption
at the GUC-stress+bgworker-launch sequence (custom-GUC RULED OUT; apply-launcher
RULED OUT as sole cause; migration RULED OUT).  0 warnings; process regress
245/245.  Force-push HELD; benchmark NOT run.  Backup: xtc-pre-rebase-202607160519.

### Regression 2b -- session 9 (cont): LATENT pre-existing launch crash, not a fiber-launch-file regression

- git diff backup(c69f69ab93a, pre-rebase) -> HEAD on the fiber launch/wait path
  (bgworker.c, launch_backend.c, pg_xtc_carrier.c, waiteventset.c, latch.c,
  launcher.c): the ONLY change is the mechanical xtc_diag_write() warning
  cleanup in pg_xtc_carrier.c -- benign, not a crash cause.  The crash-relevant
  fiber-launch code is UNCHANGED from pre-rebase.
- INFERENCE: the launch crash is a LATENT pre-existing bug, newly EXPOSED by the
  amutex deadlock fix.  Pre-amutex, 001 hung/died at the concurrent GUC-stress
  (ThreadedGUCMutex deadlock) BEFORE reaching test 56 / the thread-bgworker
  launch, so the launch crash was masked.  With the deadlock fixed, 001 now
  progresses to the launch and hits it.  So "2b" is really two bugs: the GUC
  deadlock (fixed) + a latent thread-bgworker-launch-after-heavy-fiber-activity
  crash (open).
- The MyProcPid==0 "unrecognized[0]" WaitEventSetWaitBlock fibers seen earlier
  have latches marching by 0x400 (consecutive PGPROC latches) at sequential
  timestamps -- these are pooled-park idle carriers rotating PGPROCs, likely
  BENIGN, not the crash fiber.  Do not over-index on them.

IMPASSE on the exact faulting line: (1) the crash is SIGSEGV caught by libxtc's
fault guard, which re-raises on a fiber/alt stack and does NOT dump a core even
with PG_XTC_ALLOW_CORE=1 (confirmed from 001's own postmaster) -- a known libxtc
limitation; (2) the /tmp harness (even config-matched to 001) does NOT reproduce
because it lacks 001's background_psql ordered-teardown-then-launch timing; only
that TAP path triggers it; (3) elog markers reproduce but shift the crashing
fiber (Heisenbug).

CONCRETE NEXT STEP (highest value, low risk): add a cheap fail-fast Assert/elog
at the thread-bgworker startup + WaitEventSetWaitBlock xtc-branch that verifies
the fiber's per-backend identity is coherent (MyProcPid!=0 for a real backend
wait; the WaitEventSet's owning backend == CurrentPgBackend), so 001 fails AT the
corruption point (with a PANIC that DOES core, or an elog with a stable
backtrace) instead of the downstream SIGSEGV.  Alternatively, build a
timing-faithful repro using coproc/expect persistent psql sessions finished in
order then launch, run under /tmp so a core lands.  The fix is almost certainly
in the thread-bgworker startup establishing/holding correct CurrentPgBackend/
CurrentPgExecution across the launch + first WaitLatch, OR a shared WaitEventSet/
latch lifetime bug in the bgworker fiber path.

STATE unchanged: reg 1+2a fixed; 2b GUC deadlock fixed (amutex); latent
thread-bgworker-launch crash OPEN + well-characterized.  0 warnings; process
regress 245/245.  Force-push HELD; benchmark NOT run.  Backup:
xtc-pre-rebase-202607160519 (=c69f69ab93a).

### Regression 2b -- session 9 final: gen=7 slot-reuse hypothesis (top lead for next session)

The crash fiber is consistently at a HIGH generation (gen=7 on loop=2, or
gen=2 on loop=4 in a shifted run) -- a fiber SLOT that has been reused several
times.  001's 4-worker GUC-stress churns sessions (connect/disconnect) ~50
tests' worth before the launch, so fiber slots are recycled multiple times.  The
thread-bgworker (or apply-launcher, or a GUC-stress worker) fiber that crashes
occupies a REUSED slot.  Leading hypothesis: per-fiber / per-backend state is
not FULLY reset when a fiber slot is recycled, so the Nth occupant inherits
stale state (a dangling WaitEventSet/latch pointer, stale CurrentPgBackend/
Execution binding) and faults at its first deep wait (WaitLatch ->
WaitEventSetWaitBlock).

TOP NEXT STEPS for next session (in priority order):
1. Test the slot-reuse hypothesis cheaply: reduce 001's GUC-stress session churn
   (e.g. run the stress in 1 worker, or fewer iterations) and see if the crash
   moves to a lower gen or disappears.  If churn drives it, the bug is fiber-slot
   reset.
2. Audit the fiber-slot / logical-session RESET path (backend_runtime session
   reset buckets, PgCarrierDetach/adopt, the read-command park/resume) for any
   per-backend field that is NOT cleared on slot reuse -- especially
   WaitEventSet/latch owner fields (owner_fiber_valid/loop/local/gen on the
   latch), latch_wait_set in PgBackendIPCState, and _SPI_current in PgExecution.
3. Add a fail-fast Assert at fiber slot ADOPT (PgCarrierAttachBackend or the
   backend-fiber entry) that the incoming per-backend state is zeroed/coherent,
   so 001 fails AT reuse with a clean backtrace rather than a downstream SIGSEGV.
4. Timing-faithful repro for a core: 001's background_psql ordered-finish-then-
   launch.  The bash-FIFO attempt (/tmp/t001.sh) deadlocked -- use a proper
   coproc or a small Perl script reusing PostgreSQL::Test::Cluster's
   background_psql directly, run under /tmp with PG_XTC_ALLOW_CORE=1.  (Note:
   the SIGSEGV is fault-guard-suppressed; only a PANIC path cores -- so pair
   with step 3's fail-fast Assert to force a core.)

Do NOT re-chase: custom-GUC (clean), apply-launcher-as-sole-cause (falsified),
migration (libxtc pins procs), single-vs-multi-carrier (was stray-server
contamination), the MyProcPid==0 pooled-park fibers (benign).

### Regression 2b -- session 9 RESOLVED (residual crash fixed) + test 75 is a separate cross-fiber-wake bug

ROOT CAUSE FOUND + FIXED (commit 2d96b2ccb60).  Got a real CORE by temporarily
gating xtc_fault_guard_install() behind PG_XTC_NO_FAULT_GUARD (env) so the
SIGSEGV dumped instead of being re-raised on the alt stack.  Backtrace:
  ProcessInterrupts() postgres.c:4044  pg_atomic_read_u32(&MyProc->pendingRecoveryConflicts), ptr=0x1a8
  <- errfinish (elog.c:627 CHECK_FOR_INTERRUPTS)
  <- backend_thread_run_worker launch_backend.c:1805 (the DEBUG1 ereport)
MyProc==NULL (0x1a8 = pendingRecoveryConflicts offset from NULL).  A threaded
worker/backend fiber reaches an interrupt check via an early ereport's
errfinish->CHECK_FOR_INTERRUPTS BEFORE InitProcess() installs MyProc, when a
cross-fiber interrupt is delivered in that window.  A forked process never
processes interrupts that early (signals blocked until setup), so upstream
ProcessInterrupts freely derefs MyProc.  FIX: guard the sole MyProc deref in
ProcessInterrupts (the recovery-conflict check) with MyProc != NULL.  Process
mode byte-for-byte unaffected (MyProc always set there).  Verified: the ONLY
MyProc-> deref in all of ProcessInterrupts is that one line.

IMPACT: 001 goes from dying (SIGSEGV) after test 56 -> runs 127/128 subtests.
Process regress suite green with the fix.

REMAINING: test 75 "mixed teardown stress accepted terminate requests" fails
(1/128), NO crash -- a SEPARATE, newly-reachable bug (001 never reached test 75
before because it crashed at 56).  pg_terminate_backend(pid, 5000) on 4 idle
pooled `background_psql` sessions: one specific target (deterministically the
2nd, e.g. PID 89) "did not terminate within 5000 milliseconds".  Instrumented
SendInterrupt/PgBackendWakeForInterrupt: for ALL 4 targets the PROC_DIE bit is
set fresh (old_mask=0x0, already_set=0), interrupt_latch is a valid non-NULL
shmem addr, and SetLatch(interrupt_latch) IS called -- yet one idle-parked fiber
does not wake within 5s.  => a CROSS-FIBER SetLatch wake MISS for an idle pooled
session parked in the read-command wait (Phase 17 latch/wake domain): SetLatch
on the target's interrupt_latch does not reliably xtc_proc_wake the specific
parked fiber (likely the latch's owner_fiber_{loop,local,gen} registration is
stale/unset for a between-commands parked session, or the wake targets the wrong
loop after the session's last carrier).

NEXT (test 75): audit the read-command park (PG_READ_COMMAND_PROTOCOL_PARK) +
SetLatch cross-fiber wake path -- ensure a parked idle session registers itself
as its interrupt_latch's owner_fiber before yielding (mirroring the
WaitEventSetWaitBlock xtc-branch owner_fiber capture at waiteventset.c:1381) so a
cross-fiber SetLatch xtc_proc_wakes the right parked fiber.  Compare against the
Phase 17 sem_wake_fd/ProcWaitOnSemaphore fiber-park pattern.  Two-reviewer gate
before landing both the crash fix and the test-75 fix.

STATE: reg 1+2a fixed; 2b GUC deadlock fixed (amutex a8d4a4440c0); 2b residual
crash FIXED (2d96b2ccb60); test 75 cross-fiber-wake OPEN.  0 warnings; process
regress green.  Force-push HELD until 001 fully green.  Backup:
xtc-pre-rebase-202607160519.

### test 75 ROOT CAUSE: FeBe wait-set latch owner_fiber not registered for the parking session

Instrumented SetLatch: for the terminate targets, SOME have owner_fiber_valid=1
(woken fine) and the FAILING one has owner_fiber_valid=0 (ofib=0/0/0) with
maybe_sleeping=1, sibling=1 -> SetLatch proceeds past the early return but SKIPS
the xtc_proc_wake(owner_fiber) block (guarded by `latch->owner_fiber_valid`), so
the socket-parked fiber's loop is never poked -> 5s timeout.

MECHANISM: a pooled idle session parks in PgSessionStagingWaitProtocolRead ->
WaitEventSetWait(connection->protocol.fe_be_wait_set, ...) while DETACHED
(CurrentPgBackend==NULL).  WaitEventSetWaitBlock's xtc branch registers
set->latch->owner_fiber_* -- but only if the FeBe wait_set's cached set->latch
points at the CURRENT session's MyLatch (== backend->interrupt_latch).
PgCarrierAttachBackend refreshes FeBeWaitSet's latch to MyLatch on attach, BUT
only under `backend->my_proc != NULL && backend->core.latch ==
&backend->my_proc->procLatch` (backend_runtime.c:667-674).  When that condition
is not met for a given session (or the FeBe set's latch is otherwise stale from
a prior occupant of the reused connection), the park registers owner_fiber on
the WRONG/stale latch, so SetLatch(current interrupt_latch) sees
owner_fiber_valid=0 and cannot xtc_proc_wake the parked fiber.  Session-specific
(matches the observed "some wake, one doesn't").

FIX DIRECTION (test 75): guarantee the FeBe wait_set's latch position tracks the
CURRENT session's MyLatch AND that the parking fiber is registered as that
latch's owner_fiber before the park's WaitEventSetWait yields -- either by
unconditionally refreshing FeBeWaitSet's FeBeWaitSetLatchPos to MyLatch at
protocol-park time (not only under the narrow attach condition), or by an
explicit latch_set_fiber_owner(MyLatch) at the park point (mirroring the
WaitEventSetWaitBlock xtc-branch owner_fiber capture).  Audit the
backend_runtime.c:667 condition -- why is it gated on
core.latch==&my_proc->procLatch, and is there a session where that is false at
park time?  Two-reviewer gate before landing.

STATE: reg 1+2a fixed; 2b GUC deadlock fixed (amutex); 2b residual crash FIXED
(2d96b2ccb60, ProcessInterrupts NULL-MyProc guard); test 75 (cross-fiber wake of
idle pooled session) ROOT-CAUSED, fix pending.  0 warnings; process regress
green; 001 = 127/128.  Force-push HELD until 001 green.

### 2b residual crash fix (2d96b2ccb60): TWO-REVIEWER GATE = SHIP-WITH-CHANGES (both agree)

Two independent adversarial committer-grade reviews (both read the actual source):
- BOTH: the MyProc!=NULL guard on ProcessInterrupts:4044 closes the ONLY
  reachable MyProc NULL-deref for the realistic pre-InitProcess interrupt set
  (recovery-conflict is the sole UNCONDITIONAL deref; ProcDie/QueryCancel paths
  reach LockErrorCleanup which early-returns pre-InitProcess via
  GetAwaitedLock()==NULL; ProcessRecoveryConflictInterrupts and the sibling
  HandleRecoveryConflictInterrupt are unreachable pre-InitProcess since a
  not-yet-registered fiber is not in the procarray).  SHIP to stop the crash.
- BOTH: process mode byte-for-byte equivalent (MyProc is set by InitProcess
  before interrupts are serviced; ProcKill nulls MyProc only inside shmem_exit
  where InterruptHoldoffCount!=0 already early-returns ProcessInterrupts).
- BOTH: race-free (MyProc resolves via the current fiber's CurrentPgBackend;
  test+read are sequenced on one fiber; no concurrent InitProcess).
- BOTH follow-ups (tracked, NOT blocking the crash fix):
  1. Deeper root-cause fix: HOLD interrupts across the pre-InitProcess worker/
     backend fiber startup window (so ProcessInterrupts bails at the
     InterruptHoldoffCount check for the whole window, making the sibling
     HandleRecoveryConflictInterrupt safety explicit-not-lucky and covering any
     future early-registered on_shmem_exit callback).  NOTE: a localized
     HOLD/RESUME around just the DEBUG1 ereport does NOT work (RESUME re-arms the
     next CHECK_FOR_INTERRUPTS with MyProc still NULL) -- the HOLD must span
     entry..InitProcess, which cleanly requires resuming inside/after InitProcess
     (spans functions); do it deliberately as its own reviewed commit.
  2. A threaded regression test firing a cross-fiber interrupt into the startup
     window.  COVERAGE NOW: 001 exercises this path end-to-end (was SIGSEGV at
     test 56; with the fix it runs 127/128).  check-threaded-workers evidence to
     follow with the holdoff commit.

DECISION: keep the shipped guard (crash fixed, reviewer-clean); track the
holdoff refactor + explicit test as a follow-up commit under its own review gate.

### Session 9 summary: crash FIXED + 003 FIXED; test 75 (cross-fiber terminate-wake) is the last 001 failure

RESULT: test_backend_runtime suite 11/14 -> only 001 fails (on ONE subtest,
test 75), no crashes anywhere.  Was 2 crashing/erroring tests (001 dying at
test 56, 003 dying) at session start.

Committed this session:
- 2d96b2ccb60  fix(threaded): ProcessInterrupts NULL-MyProc guard (residual 2b
  crash). Two-reviewer SHIP-WITH-CHANGES (both agree it closes the crash; the
  HOLD-interrupts-window durability refactor + explicit test are tracked
  follow-ups).  001: 56 -> 127/128.  Process regress green.
- b4fa9c8ad4d  fix(test): 003 stale expectations (process-only rejection message
  regex + pooled-reclaim log wording).  003 now 43/43.

test 75 "mixed teardown stress accepted terminate requests" -- OPEN, precisely
root-caused this session (diagnostics reverted, NOT shipped):
- Symptom: pg_terminate_backend(pid, 5000) on 4 idle pooled `background_psql`
  sessions; one target (deterministically PID 89) "did not terminate within 5000
  ms".  No crash.  A SINGLE idle-at-command-read session terminates FINE
  (verified /tmp/term2.sh -> TERMINATE=t), so it is the mixed-stress interaction.
- Confirmed via instrumentation: the failing target's parked fiber NEVER observes
  a WL_LATCH_SET wake (XTCWAKEOBS pid=<target> count = 0) while the terminate's
  SendInterrupt sets PROC_DIE fresh (old_mask=0) and SetLatch(interrupt_latch)
  IS called.  The cross-fiber xtc_proc_wake of the parked fiber's loop is MISSED.
- Earlier SetLatch instrumentation: the failing target had owner_fiber_valid=0 on
  its interrupt_latch at SetLatch time -> the xtc_proc_wake block was skipped.
- Tried (did NOT fix, reverted): (a) refresh the FeBe wait_set's latch to
  backend->core.latch at park time -- confirmed core.latch==interrupt_latch, so
  the latch identity is CORRECT; (b) explicitly LatchSetCurrentFiberOwner() on
  the interrupt_latch at park start (before the pre-block poll).  Neither made
  the target observe a wake.  => the miss is NOT latch identity and NOT (only)
  a missing owner_fiber registration at the block point; the parked fiber's loop
  is not being poked, or owner_fiber is stale/cleared between registration and
  the terminate, or the fiber is momentarily not parked (poll/setup window) when
  the stress fires the terminate.

NEXT (test 75): this is a Phase-17-class cross-fiber wake race on the pooled
read-command park under concurrent mixed teardown.  Instrument with the fault
guard disabled + a stable pid tag (backend id, not proc_pid which is 0/shared
for pooled) to confirm whether (1) xtc_proc_wake is even called for the target
(owner_fiber_valid at SetLatch), (2) xtc_proc_wake's rc, and (3) whether the
target fiber is PARKED vs mid-poll when the terminate fires.  Compare with the
Phase 17 sem_wake_fd eventfd park (proc.c ProcWaitOnSemaphore) which solved the
same class for semaphores -- the read-command park may need the same eventfd
secondary wake rather than relying on owner_fiber+xtc_proc_wake.  Two-reviewer
gate.  NOTE: a single idle session terminates fine, so the fix must target the
concurrent-teardown timing specifically.

STATE: reg 1+2a fixed; 2b GUC deadlock fixed (amutex); 2b residual crash FIXED
(reviewer-clean); 003 fixed; test 75 OPEN (root-caused).  0 warnings; process
regress green; test_backend_runtime 11/14 (only 001, only test 75).  Force-push
HELD until 001 green.  Backup: xtc-pre-rebase-202607160519.

### test 75 -- session 10: deep timeline established, NOT isolable, fix attempts falsified

Definitive timeline (timestamped instrumentation, fault guard off, reverted):
- The failing terminate target's parked fiber wakes ~5s LATE -- on its park
  timeout, NOT on the terminate's SetLatch.  Sequence for the target:
  PARK (mailbox=0x0, owner_fiber_valid=1) -> SENDPROCDIE sets PROC_DIE + calls
  the wake (wake=1, ilatch matches park latch, ilatch_is_set=0) -> fiber does
  NOT wake -> ~5s later WOKE on timeout.  pg_terminate_backend(pid,5000) times
  out just before.
- SetLatch's owner_fiber wake block (SETLATCHWAKE, xtc_proc_wake rc=0) fires
  ~57x per run for OTHER latches and WORKS; the failing target's SetLatch does
  NOT reach it.  So it is a MISS on one specific target, deterministically one
  per run (but a different backend id each run).

Fix attempts this session -- ALL BUILT CLEAN, ALL FALSIFIED (reverted):
1. Re-issue xtc_proc_wake on the SetLatch `is_set` early-return path.  No effect
   (ilatch_is_set was 0 at the miss, so not the is_set path).
2. Broaden that to also WakeupOtherProcFd/pthread_kill on is_set.  No effect.
3. Treat a registered cross-fiber owner (owner_fiber_valid, same proc, other
   thread) as sibling_thread_owner so SetLatch does not bail at
   `!maybe_sleeping && !sibling_thread_owner`.  8/8 still fail -> SetLatch is
   NOT bailing at that gate for the target.
4. Refresh FeBe wait_set latch to core.latch at park + explicit
   LatchSetCurrentFiberOwner at park start (earlier session).  No effect;
   confirmed core.latch==interrupt_latch.

NOT ISOLABLE: reproduced ONLY in a full 001 run.  Standalone repros that PASS:
- single idle-at-command-read session terminate (/tmp/term2.sh) = t
- 4 idle sessions + bool_and(pg_terminate_backend(pid,5000)) (/tmp/term4.sh) = t
- FULL mixed stress (4x fatal + abandoned advisory-lock + terminate targets)
  from a FRESH server (/tmp/term_mixed.sh) = t
=> the miss requires the ~50 prior 001 subtests (GUC stress, crash-recovery,
protocol-scheduler, prior terminate waves) to warm/churn the pool+carriers into
the triggering state.  Same "only in the full sequence" wall as the crash had.

ASSESSMENT: test 75 is a genuine, deep, full-context-dependent cross-fiber
latch-wake race on the pooled read-command park.  The wake path is correct in
the common case; one target per full-001 run misses and waits its park timeout.
The observed facts (SetLatch called on the right latch with is_set=0 and
owner_fiber_valid=1, yet no wake reaches the block) are internally inconsistent
with a simple gate/identity bug -- suggesting either (a) the owner_fiber values
read by SetLatch are stale vs the fiber's CURRENT park generation (the fiber
re-parked with a new gen between the mailbox-empty wake and the terminate, and
SetLatch xtc_proc_wakes the OLD gen -> libxtc drops it), or (b) a
read-after-write visibility gap on owner_fiber_* / maybe_sleeping across
carriers.  (a) is the leading theory and matches "wakes on timeout not signal".

RECOMMENDED FIX (next session, review-gated): the robust, race-free path is the
Phase 17 pattern -- a per-backend eventfd that SendInterrupt WRITES and the
pooled read-command park ALWAYS includes in its FeBe wait set (level-triggered),
instead of relying on owner_fiber+xtc_proc_wake (edge-triggered, gen-sensitive).
This is exactly how ProcWaitOnSemaphore/sem_wake_fd solved the semaphore version
(proc.c).  Alternatively, confirm/fix theory (a): make SetLatch's owner_fiber
read + the park's owner_fiber write use a generation/seqlock so a stale-gen
xtc_proc_wake cannot be silently dropped.

STATE: reg 1+2a fixed; 2b deadlock fixed; 2b crash FIXED (reviewer-clean); 003
fixed (43/43); test 75 OPEN -- deep cross-fiber-wake race, root-caused to the
timeline above, 4 fix attempts falsified, eventfd-secondary-wake is the
recommended fix.  0 warnings; process regress green; test_backend_runtime 11/14
(only 001, only test 75).  Force-push HELD.  Backup: xtc-pre-rebase-202607160519.

### test 75 FIXED (Phase 17 eventfd) + full-suite failures are PRE-EXISTING

test 75 fixed by commit 103b994635b (per-PGPROC interrupt_wake_fd eventfd; the
Phase 17 sem_wake_fd pattern applied to the pooled read-command park).  Two
adversarial reviews: item-1 (stale fd across pooled reuse) is a non-issue
(thread-per-session: 1 connection = 1 PGPROC = 1 FeBeWaitSet for life);
reviewer 2 caught a real defect (the pos-3 eventfd, once added to the shared
FeBe set, leaked into secure_read/secure_write/SocketBackendStickyIdleWait which
did not drain it -> spin) -- FIXED by draining pos-3 + treating it as a latch in
those harvest sites.  Reviewer-1 comment fix applied.  001 = 128/128;
test_backend_runtime 12/14 OK 0 Fail; process regress green; 0 warnings.

FULL `meson test` (all 413 tests) shows 52 failures, ALL in logical-replication-
adjacent suites (subscription/*, recovery/*, pg_upgrade/*, test_decoding,
pg_basebackup/pg_recvlogical, postgres_fdw).  VERIFIED PRE-EXISTING, not caused
by the test-75 fix: restored the 6 changed files to the PARENT commit (f8b2d345),
rebuilt, and subscription/004_sync STILL fails identically (logical replication
tablesync worker SIGSEGV).  These run in PROCESS mode (multithreaded defaults
false; subscription tests do not set it) and my fix is fully USE_XTC_CARRIER-
guarded + the pos-3 eventfd is never added in process mode, so it cannot affect
them.  They are a separate pre-existing logical-replication issue on this branch,
outside the test-75/2b scope and outside the threaded green gate
(gmake check / check-threaded / test_backend_runtime).

STATE: reg 1+2a fixed; 2b deadlock fixed; 2b crash FIXED (reviewer-clean); 003
fixed; test 75 FIXED (reviewer-clean).  test_backend_runtime GREEN (12/14, 0
Fail).  process regress GREEN.  0 warnings.  Pre-existing logical-rep suite
failures noted, out of scope.  Ready to force-push + benchmark.

### Benchmark (local A/B, post-fix, branch GREEN + force-pushed)

Dev-host (8 CPU), scale 50, shared_buffers=1GB, max_connections=200, fsync=off,
synchronous_commit=off, 16 clients / 8 jobs, prepared.  Process lane then
threaded lane (multithreaded=on, pooled_protocol_carriers=8), same data dir.

- Read-only SELECT (-S -M prepared):
    process  = 151,039 tps  (many backend processes)
    threaded = 170,133 tps  (+12.6%; SINGLE process, nprocs=0 -- all fibers)
  => threaded BEATS process on read-only at carriers==cpus, consolidated to one
     process.
- Write TPC-B: process = 17,067 tps.  Threaded write lane did NOT complete within
  the local command timeouts (dev-host meson-on-btrfs is too slow/contended for
  reliable timed write runs; heavy write/saturation belongs on EC2).

The threaded server ran cleanly end-to-end (fibers spawn/exit normally;
NOTIFY-parked-client + terminate paths correct) -- Phase 17 fix validated live.

Full apples-to-apples saturation + p50/p95/p99 + HammerDB: EC2 external-driver
harness (src/tools/benchmark/mtpg_remote_bench.sh + mtpg_ec2_ab_provision.sh) on
a big-core box, fresh session.

STATE: xtc GREEN (test_backend_runtime 12/14 0-Fail, process regress green, 001
128/128, 0 warnings), test 75 two-reviewer-clean, FORCE-PUSHED (5f6370ceded).
Local read-only A/B: threaded +12.6%, single-process.  EC2 saturation = next.

### pg_ctl -m fast stop (threaded) hang -- investigated: already fixed by the test-75 eventfd

The prior EC2 benchmark agent wedged on a bare `pg_ctl -m fast stop` against a
threaded server. Investigated whether this is a real threaded-shutdown bug:

- Fast shutdown path: postmaster SIGINT -> PM_STOP_BACKENDS -> SignalChildren(SIGTERM)
  -> signal_child(). For a threaded/logical child, signal_child() routes through
  thread_child_signal_interrupt() -> PostmasterChildRaiseThreadInterrupt() ->
  **SendInterrupt(logical_backend, PROC_DIE)** (pmchild.c:449). This is the SAME
  SendInterrupt/PgBackendWakeForInterrupt path the test-75 fix (commit 103b994635b)
  hardened with the per-PGPROC interrupt_wake_fd level-triggered wake.
- PostmasterChildHasLogicalBackendPublication() is true for BOTH pooled-logical
  fibers AND thread-carrier aux workers (checkpointer/bgwriter), so the shutdown
  signal to idle parked backends AND aux carriers goes through the fixed wake.
- REPRODUCED locally on the current branch (has the fix): threaded server,
  pooled_protocol_carriers=4, 3 IDLE parked connections, then
  `pg_ctl -m fast -w -t 40 stop` -> **rc=0, elapsed 0.12s (CLEAN, no hang)**.
- The prior wedge was the benchmark agent's AD-HOC smoke test using a BARE
  `pg_ctl stop` with NO CHECKPOINT and NO timeout, waiting forever on the
  shutdown checkpoint of dirty buffers. The harness's own stop_pg (CHECKPOINT +
  `pg_ctl -m fast -w -t 600` + kill fallback) does not wedge; the re-run agent is
  instructed to use it and wrap all calls in `timeout`.

CONCLUSION: no separate threaded-fast-stop bug on the current branch; the
test-75 eventfd fix already makes idle-backend fast shutdown wake deterministically.
(Under-load fast-stop could not be timed cleanly on the slow dev-host; idle case
is clean and the code path is the fixed one.)

### Metal m8idn.metal-96xl fair st-vs-mt (2026-07-20): auto-256 cap NOT the cause; WAL-lock is

Corrected-methodology run (carriers set EXPLICITLY 192/256/384/512, NOT auto;
256GB shared_buffers huge_pages=on BOTH lanes; DB on 6xNVMe RAID-0; DURABILITY=on;
external saturating driver; %steal=0; ratio-validated 0.435 every cell):
- VU=192: fork 1,348,238 NOPM vs mt best 783,085 (c=256) = 58%
- VU=384: fork 1,039,281 vs mt best 667,534 (c=512) = 64%
- 256->384 carriers did NOT help (783k->745k) => scheduling width is NOT the
  limiter; the auto-256 cap does NOT explain the gap.  mt RAM <= fork (PSS 972
  vs 1031 MB).
- NEITHER lane saturated CPU (fork ~30%, mt ~15%): DURABILITY=on TPC-C is
  WAL-insert-lock bound on this box, so ~100% CPU is unreachable with fsync on.
  The gap is a WAL/lock-serialization gap in the fiber runtime, wider under
  durability-on than the earlier durability-off ~86-90%.
- HammerDB monitor VU hit FINISHED_FAILED on every threaded cell (workers OK) ->
  NOPM captured server-side; a HammerDB CLI measurement artifact, not a PG fail.

TWO REAL THREADED-MODE BUGS surfaced (harness worked around; runtime owner TODO):
1. **pipe() -> SEGFAULT at VU=384**: threaded runtime shares ONE process fd table
   across all fiber backends; per-backend fd use that is fine in fork mode
   exhausts nofile, and the self-pipe pipe() failure
   (waiteventset.c:315 InitializeWaitEventSupport, elog(FATAL)) does NOT cleanly
   terminate the fiber -> segfault (same early-startup-fiber FATAL-doesn't-unwind
   class as the 2b ProcessInterrupts NULL-deref). Workaround was nofile=1M.
   ROOT FIX options: (a) make FATAL from an early backend-fiber (pre-full-init)
   unwind/exit cleanly instead of crashing; and/or (b) don't allocate a per-fiber
   self-pipe pair -- N backends x2 fds exhausts the shared table; the latch
   self-pipe/signalfd could be per-carrier or use the existing per-PGPROC
   interrupt_wake_fd/sem_wake_fd eventfds. Investigate which.
2. start-gate grepped a log banner this build doesn't emit -> TCP-probe gate
   (harness fix, committed cdbebc3ae43).

NEXT (agreed plan): (a) fix the pipe()->segfault (fail-closed, not crash);
(b) ONE diagnostic mt run at VU=384 DURABILITY=OFF on the metal with perf, to
isolate the scalability limit at true ~100% CPU (durability-off removes the WAL
fsync bound so we see the scheduler/lock ceiling). Profile the WAL-insert /
lock-wait path (perf flamegraph) to name the top serializer.

### DONE 2026-07-20: pipe()->segfault FIXED + diagnostic durability-OFF run

(a) **pipe()->segfault FIXED** (commit 15588c2012b, pushed). Root cause: a
pooled-protocol carrier (a long-lived pthread, NOT a per-session fiber) called
InitializeWaitEventSupport() whose self-pipe pipe() failed under fd exhaustion
with elog(FATAL); FATAL runs PgBackendExit -> per-backend cleanup on a
non-backend thread -> backend_thread_exit publication==NULL branch
pthread_exit()s the runtime-critical carrier thread -> whole server dies
(SIGSEGV under the fault guard when a sibling fiber then touches disrupted
state). Same early-startup FATAL-doesn't-unwind class as 2b. FIX: create the
carrier self-pipe on the POSTMASTER thread in
backend_pooled_protocol_start_one_carrier() (failure -> return false ->
connection refused, runtime intact), hand fds to the carrier via
WaitEventSetPresupplySelfPipe(); InitializeWaitEventSupport() adopts them,
process mode byte-for-byte unchanged. Reproduced+verified fail-closed 3 ways
(forced fail, injected, real ulimit -n 500). Two-reviewer gate: codex SHIP,
maki SHIP-WITH-CHANGES (annotation + kqueue guard, both added).

(b) **Diagnostic durability-OFF run, m8idn.metal-96xl (192c/384vCPU/1.5TB,
us-east-1d, %steal=0), VU=384, 256GB SB huge_pages=on, DB on 6xNVMe RAID-0,
WAREHOUSES=1000, external c7i.24xlarge driver:**
- fork/process: 1,354,571 NOPM (3,115,565 TPM), peak CPU 19.4%, PSS 2522 MB
- threaded c=384: 685,053 NOPM (1,577,393 TPM), peak CPU 21.4%, PSS 1908 MB
- **mt/fork = 50.6%.** NEITHER lane saturated CPU even durability-OFF
  (fork 19.4%, mt 21.4% peak) -- the box is MOSTLY IDLE, so ~100% CPU is
  unreachable and the limit is a serialization/blocking ceiling, NOT CPU.

**PERF NAMES THE SERIALIZER (refutes the WAL-insert-lock hypothesis for
durability-off):**
- threaded top non-idle self symbol = **`__xtc_exec_try_steal` 18.76% self**
  (the libxtc fiber scheduler's work-stealing spin), + io_uring_enter/
  io_cqring_wait (12-14%), with **54% of all samples in CPU IDLE**
  (swapper/mwait_idle). Carriers busy-scan sibling run-queues for work while
  the box sits idle: the xtc executor is not distributing/feeding work to
  carriers fast enough. NO LWLock/WALInsert symbol appears in the threaded
  top -- WAL is NOT the durability-off serializer.
- fork top userspace self symbol = **`LWLockAcquire` 9.98% self** + real query
  work (_bt_compare 3.86%, PinBuffer 3.57%) + kernel scheduler load_balance/
  update_sg_lb_stats 6.54% (Linux balancing 384 processes). Fork is
  LWLock+Linux-scheduler bound, also at only ~19% CPU.

**HEADLINE: the mt scalability limit at VU=384 durability-off is the libxtc
fiber-scheduler work-distribution/steal loop (`__xtc_exec_try_steal`), NOT a
WAL-insert lock and NOT a CPU ceiling.** With 384 carriers, ~19% of all cycles
burn in the steal spin while 54% of the machine idles -- the scheduler cannot
keep carriers fed. NEXT: investigate xtc executor work-distribution (steal
heuristics / carrier count / run-queue handoff) before touching any PG lock;
the durability-ON WAL-lock hypothesis is separate and unconfirmed by this
durability-off profile. All EC2 instances terminated and verified.

### WAL / spinlock fiber-awareness -- agreed plan (2026-07-20)

DECISION: do NOT speculatively convert any lock. The mt OLTP gap (metal:
58-64% of fork, durability-on, WAL-bound) is HYPOTHESIZED to be
insertpos_lck (the slock_t WAL-reservation spinlock, xlog.c:396) serializing
worse under cooperative fiber scheduling than under fork, but this is NOT yet
profiler-confirmed.

Key distinction:
- LWLocks (WALInsertLocks x8, buffer content locks, ...) are ALREADY partly
  fiber-aware: Phase 17's ProcWaitOnSemaphore/sem_wake_fd parks the fiber on an
  eventfd on the deep-wait path instead of blocking the carrier. Residual cost
  is wait/wake overhead, not a hard block. Largely handled.
- SPINLOCKS (slock_t / s_lock) are the fiber-hostile primitive: no yield point,
  a spinning fiber pins its carrier, and a descheduled holder causes convoying.
  insertpos_lck is the leading candidate (contended + hot OLTP write path). But
  spinlocks guard TINY critical sections -- converting a short-hold spinlock to
  a yielding lock adds park/wake cost that can be WORSE than the spin, and a
  yield inside a spinlock-protected section can deadlock/corrupt. High risk.

PLAN:
(A) WAIT for the Part B perf run (durability-off, VU=384, flamegraph) to NAME
    the top self-time symbol. If s_lock/perform_spin_delay on insertpos_lck ->
    confirmed; convert THAT one (review-gated, A/B neutral-or-better per the
    Phase-18 rule). If LWLockAcquire/xtc_yield on WALInsertLocks -> tune the
    existing Phase-17 park / add insert locks. If something else -> follow it.
(B) PREP (read-only, no code change): survey every contended slock_t + its
    fiber-safety -> a design map: which spinlocks are convertible, which are
    too-short-to-convert, which want a LOCK-FREE/atomic (CAS) rewrite instead
    (e.g. insertpos_lck -> atomic CurrBytePos advance, upstream-explored,
    sidesteps the spinlock-vs-fiber problem with no park/wake cost). Have this
    ready so the moment the profile lands we convert the RIGHT lock the RIGHT
    way.

Preferred fix for a confirmed hot spinlock guarding a tiny section: lock-free
atomic reservation (CAS), NOT a yielding lock -- yielding costs more than the
spin for short holds. A yielding (xtc_amutex-style) lock only fits locks with
longer or sleep-capable critical sections.

### Spinlock fiber-safety survey RESULTS (2026-07-20, read-only design map)

Full doc: /tmp/spinlock_fiber_survey.md (condensed here for durability).

Ranked payoff for the mt OLTP gap (58-64% of fork):
1. insertpos_lck (xlog.c:396, WAL cursor CurrBytePos/PrevBytePos) -- ~40-60% of
   the gap. SINGLE GLOBAL spinlock on EVERY insert/update/delete, ~50-200ns hold,
   VERY HIGH contention + fiber-hostility. FIX = LOCK-FREE CAS (atomic advance of
   CurrBytePos/PrevBytePos), NOT a yielding lock (park/wake ~1-5us > 50-200ns spin
   -> would be worse). Upstream precedent: logInsertResult/logWriteResult atomics.
2. LockBufHdr (bufmgr.c:~2300, per-buffer header) -- ~20-30%. Very hot, very
   short. MONITOR; if confirmed, xtc_amutex or lock-free/per-cache-line.
3. buffer_strategy_lock (freelist.c:35, clock-sweep hand) -- ~5-15%. MONITOR.
4. LWLock deep-wait -- ALREADY solved (Phase 17 sem_wake_fd fiber park).
5. WALInsertLocks (x8) fast-path spin -- low; Phase 19+ scheduler-aware backoff.
LEAVE (uncontended / too-short): info_lck, FastPathStrongRelationLocks->mutex,
msgnumLock, dynahash freelist mutex.

CRITICAL: a GENERAL s_lock/perform_spin_delay "yield after N spins" is NOT SAFE
-- a fiber yielding while holding a spinlock can hand its carrier to another
fiber wanting the SAME lock -> deadlock. Spinlock protocol assumes no reschedule.
Phase 17's LWLock park is safe only because it is a designed deep-wait boundary
AFTER spinlock release. => conversions must be PER-LOCK; short-hold spinlocks
want LOCK-FREE rewrites, not yielding locks.

Proposed CAS reservation sketch (ReserveXLogInsertLocation):
  do { start = pg_atomic_read_u64(&Insert->CurrBytePos);
       /* compute end from start+size, page-boundary logic */ }
  while (!pg_atomic_compare_exchange_u64(&Insert->CurrBytePos, &start, end));
  -- must preserve PrevBytePos semantics + page-crossing logic; validate no
  off-by-one; A/B gate (durability-off TPC-B, neutral-or-better) + two reviewers.

STILL GATED ON: the Part B perf flamegraph (durability-off VU=384) naming the top
self-time symbol before ANY conversion. If s_lock/perform_spin_delay on
insertpos_lck -> do the CAS. Else follow the survey per-lock.

### Next: diagnose __xtc_exec_try_steal + 54% idle (libxtc-bug vs our-misuse) (2026-07-20)

Durability-off VU=384 metal perf named the mt limit: __xtc_exec_try_steal 18.76%
self + io_uring 12-14% + 54% IDLE (mt 685k NOPM = 50.6% of fork 1.35M; box
NOT CPU-bound). Refutes WAL-lock for the durability-off case. The bottleneck is
the libxtc fiber scheduler's work distribution: carriers spin-scan sibling
run-queues for work while half the machine sits idle -> runnable work is not
being placed on idle carriers (and/or the io_uring completion -> carrier wake
path leaves carriers idle).

PLAN (agreed): diagnose the exact cause; if it's a libxtc bug/perf issue ->
write a report for the libxtc team (to /tmp, evidence-only, per the standing
verify-before-requesting rule). If it's OUR misuse of libxtc (carrier/exec
config, how we spawn/park backend fibers, io_method, wake path) -> fix in our
branch + re-test. WAIT on this diagnosis+fix before re-benchmarking.

Inputs for the diagnosis:
- libxtc source local: /home/gburd/ws/xtc (src/evt/exec.c __xtc_exec_try_steal,
  src/evt/loop.c, src/inc/deque.h). Our pin: flake.nix rev
  1ceab785c8fd9ab6056999b387d21eb9e378e8b0 (v1.23.3).
- Our carrier/executor setup: src/backend/postmaster/pg_xtc_carrier.c
  (g_xtc_exec N-loop executor when carriers>1; xtc_proc_spawn_monitor pins
  procs=pinned tasks -> NOT work-stolen; so why is __xtc_exec_try_steal hot?).
  Key question: pinned backend fibers are NOT stealable, so the steal loop is
  scanning and finding nothing (wasted spin) while carriers with a runnable
  pinned fiber can't hand it off -> idle. Is the executor's idle/steal policy
  wrong for a PINNED-proc workload? Should carriers block/park instead of
  spin-steal when their own run-queue is empty and peers' work is pinned?
- io_uring 12-14%: io_method=sync was forced; check whether the AIO/io_uring
  completion path (xtc_io) wakes the owning carrier promptly or leaves it in
  io_cqring_wait while runnable fibers wait elsewhere.

### Why pinned-per-session; the full-work-stealing blocker; libxtc-adoption roadmap (2026-07-20)

USER DIRECTION: move toward full work-stealing (don't pin backend fibers; let
libxtc rebalance), and find/fix other ways we're not fully embracing libxtc.

WHY WE'RE PINNED (the blocker, not a chosen end-state): PG's pervasive
per-backend __thread/TLS "current-work" state -- MyProc, CurrentPgBackend,
CurrentPgExecution, error/memory-context stacks, MyLatch,
PgRuntimeCurrentBridgeState (all PG_THREAD_LOCAL bound to the CARRIER OS thread).
A fiber work-stolen A->B mid-execution reads carrier B's TLS, not its own ->
crash/corruption (the exact class that broke the test-75 diagnosis). We
save/restore this state only at SPECIFIC wait boundaries (xtc_pg_wait_fd,
pg_fdatasync), NOT on every fiber switch. libxtc pins procs (xtc_proc_spawn ->
pinned=1) precisely to give shard-affinity so PG's TLS stays coherent. So
pinning is a CORRECTNESS crutch for missing migration-safety.

ENABLER for unpinning (the real work): register a PG per-backend
fiber-ctx save/restore hook via libxtc's __xtc_fiber_ctx_save/restore (proc.c
already uses these to keep __current_proc correct across yield; PG registers
NONE for its OWN current-work). A hook that saves/restores ALL per-backend
current-work on EVERY context switch would make a stolen fiber read its own
state on resume -> unpin becomes safe. Cost: ~6-pointer save/restore per switch.
Must inventory EVERY piece of per-backend TLS needing migration-safety.

CHEAP INTERIM WIN (maybe, no unpin): the 54% idle + __xtc_exec_try_steal 18.76%
may be idle carriers HOT-SPINNING in the steal loop instead of PARKING when all
peer work is pinned (libxtc idle policy / a knob) + load IMBALANCE from pinning
concentrating sessions on some carriers. Parking idle carriers + balancing
session->carrier assignment could reclaim the wasted steal CPU without the risky
unpin. Under investigation (agent 1fdbcfc9).

Deliverable: /tmp/libxtc-model-adoption-roadmap.md ranking (i) interim
park/balance win, (ii) the fiber-ctx-hook enabler -> unpin, (iii) other
libxtc-adoption gaps (primitives we reimplement vs adopt; io_uring/AIO idiom;
work-stealing-executor-vs-pinning design mismatch). No speculative unpin; the
migration-safety hook is a DESIGNED, review-gated next step.

### Fiber-ctx-hook: make per-backend current-work track the fiber, not the carrier OS thread (2026-07-20)

USER DECISION: implement the targeted ctx-hook (make the thread-local CACHE of
the 6 root pointers track the CURRENT FIBER on every switch, instead of being
bound to the carrier OS thread). Measure the per-switch cost. Then the
work-stealing-migration concern (spinlock-hold + stolen fiber) becomes
measurable. Also: transition errno/signal handling to libxtc's fiber APIs;
audit + migrate any raw __thread not behind the bridge; use libxtc's OpenSSL/TLS
abstraction; watch for other 3rd-party thread-bound issues.

MECHANISM (confirmed in source):
- libxtc calls __xtc_fiber_ctx_save()/__xtc_fiber_ctx_restore(void*) on EVERY
  coro switch (loop_int.h:279-280; coro_fctx.c:435/441/465/470). PG registers
  NONE for its own state (only libxtc's proc layer registers __xtc_proc_ctx_*).
- Our per-backend current-work = PgRuntimeCurrentBridgeState + 6
  PG_THREAD_LOCAL HotRefThreadRef cells (backend_runtime.c:100-118), all derived
  from 6 roots (runtime/carrier/backend/session/connection/execution). ~130 hot
  fields derive from these via the checked .def bridge.
- ENABLER: a chained PG fiber-ctx hook that, on switch, saves the 6 roots for
  the outgoing fiber and restores+rebinds them (incl. rebinding `carrier` to the
  executing loop -- the one non-derivable field) for the incoming fiber, then
  refreshes the bridge cells. This makes a migrated/stolen fiber read ITS OWN
  state -> the correctness prerequisite for UNPINNING.

STAGED PLAN (each stage gated):
(1) AUDIT (read-only): every PG_THREAD_LOCAL/__thread in PG + our code; classify
    behind-the-bridge vs raw. For raw ones NOT covered by the 6-root bridge,
    plan migration into per-backend session state. Distinguish libxtc TLS:
    __os_tls_* (os_thread.h) = thread-local-STORAGE abstraction (for OpenSSL &
    other 3rd-party per-thread state); xtc_tls_* (io_ext.h) = TRANSPORT-layer-
    security (crypto), NOT storage -- do not conflate. errno/signal: find
    libxtc's per-fiber errno/sigmask handling and the sites to transition.
(2) MICROBENCH: cost of the per-switch save/restore+bridge-refresh at realistic
    switch rates. Is a lazy/generation-checked refresh needed (only re-derive a
    field cache on first access after a switch, keyed on a per-fiber gen) to
    avoid eating the win? Decide before wiring the hook broadly.
(3) IMPLEMENT the fiber-ctx hook (chained via __xtc_fiber_ctx_save/restore),
    keep process mode byte-for-byte (hook only installed under the threaded
    carrier), 0 warnings, test_backend_runtime 12/14 + regress green, TWO-
    REVIEWER gate. This alone should be correctness-neutral WHILE STILL PINNED
    (validates the hook without changing scheduling).
(4) THEN measure: with the hook in place, is unpinning safe? The spinlock-hold-
    while-stolen hazard -- a fiber holding a raw slock_t must NOT be migrated
    (no yield point). Confirm libxtc only switches at yield points (a fiber
    holding a spinlock isn't at a yield point, so won't be stolen mid-section) OR
    add a guard. Measure, don't assume. Unpin is a SEPARATE later stage after
    the hook proves neutral + the spinlock hazard is cleared.
(5) errno/signal + OpenSSL(__os_tls_*) + other 3rd-party TLS migration: separate
    sub-tasks, each measured/reviewed.

Do NOT unpin in the same change as the hook. Do NOT re-benchmark the big run
until a landed, gated change warrants it (user gates).

### Fiber-ctx-hook AUDIT + COST results (2026-07-20) -- viable, MUST be lazy

Docs (in /tmp, transient): thread_bound_audit.md, fiberctx_cost.md (+ harness
/tmp/fiberctx_bench/bench2.c). Condensed here for durability.

CORRECTNESS SURFACE (small, mostly already solved):
- The 6-root bridge owns 100% of per-backend session state: 257 derived slots +
  230 session-GUC rebinds, all with owner-token LAZY re-derivation ALREADY built
  in (backend_runtime_current.h:253-281 variable##MaybeRef carry owner tokens,
  re-derive on stale token). So the hook only needs: save/restore 6 roots +
  rebind `carrier` to the executing loop + invalidate (bump per-fiber gen).
- NO unbridged per-backend session state exists. Every raw __thread is
  legitimately-per-carrier (xtc_in_backend_fiber, log throttles, self-pipe fds,
  pglz scratch, retained_top_memory_allocated), bootstrap-fallback
  (early_*_fallback), or process-mode-only. Nothing to migrate to a struct.
- errno + sigmask ALREADY fiber-safe on the normal cooperative-yield path:
  Linux fcontext swaps regs+SP; PG reads errno synchronously before any yield
  (yield is at WaitEventSetWait, outside the errno span); threaded backends keep
  a fixed all-blocked mask + latch-routed interrupts (per-txn unblock guarded
  out, postgres.c:7259). => do NOT need to transition to a libxtc errno/signal
  API for cooperative yields. (Involuntary-preemption errno save is optional;
  libxtc Linux sched is cooperative.)
- OpenSSL per-OS-thread error queue is the real 3rd-party hazard: fix = guard
  the ERR_clear_error()...ERR_get_error() span as NO-YIELD/NO-STEAL (already
  contains no yield point), NOT route through __os_tls_* (a pthread-TLS-key
  wrapper = still per-OS-thread, wouldn't fix OpenSSL's internal queue).

COST (measured -O2, /tmp/fiberctx_bench/bench2.c):
  roots-only 0.8ns | eager-full (257 fields + 230 GUC rebinds) 3832ns | lazy 5ns.
  GUC rebind = 97% of eager (230 x ~16ns dynahash lookup+store). At ~1M
  switches/s (~20 yields/txn x 50k TPS): EAGER = ~3.8 cores (377%) overhead ->
  would ERASE the work-stealing win. LAZY = ~0.4% (~760x cheaper).
  => DESIGN: LAZY / generation-checked. Hook saves/restores 6 roots + rebinds
  carrier + bumps a per-fiber gen; fields re-derive on first touch (already
  implemented via owner tokens); the GUC rebind is the ONLY unguarded piece ->
  either route the ~230 GUC reads through the session accessor directly (removes
  cost) or defer via the gen check.

RANKED no-steal guards to add AT THE UNPIN STAGE (not now): (1) OpenSSL
ERR_clear->ERR_get span; (2) error-recovery sigprocmask(UnBlockSig)
postgres.c:5493/7065; (3) unpack_sql_state static buffer; (4) errno under
preemption (optional). All clearable when we unpin.

NEXT: implement the LAZY fiber-ctx hook WHILE STILL PINNED (correctness-neutral;
save/restore 6 roots + carrier rebind + gen-invalidate; solve the GUC-rebind
cost via direct session accessor or gen-defer). 0 warnings, test_backend_runtime
12/14 + regress green, TWO-REVIEWER gate. Unpin + the no-steal guards are a
SEPARATE later stage. No big re-benchmark until a gated change warrants it.

### RESOLVED: errno/signals are runtime-owned; GUC session-rooting; TLS opts gap (2026-07-20)

libxtc clarification from the maintainer resolves items (2)/(3):
- SIGNALS are a runtime concern libxtc OWNS: (a) fault signals (SEGV/BUS/FPE/ILL)
  -> per-fiber "let it crash" containment delivering DOWN (our PG_XTC_NO_FAULT_GUARD
  interacts with this), escalates loudly on critical-section/stack-overflow faults;
  (b) preemption via per-thread SIGVTALRM (safe-point yield only); (c) loop/pool
  threads spawn with ALL signals blocked so process-directed SIGINT/SIGTERM land on
  the MAIN thread. This matches our threaded-backend assumptions (fixed all-blocked
  mask + latch-routed interrupts). => NOTHING to migrate; just confirm our
  postmaster SIGTERM/SIGINT + fault-guard handlers compose with libxtc's model.
- errno: libxtc does NOT expose errno; you work in XTC_E_* space; wrappers capture
  errno right after each syscall and convert BEFORE any yield, handle EINTR/EAGAIN
  internally, and the __xtc_unsafe_enter/leave allocator bracket keeps errno/alloc
  consistent across the signal boundary. errno is safe UNLESS our OWN code reads
  errno from a RAW libc syscall AFTER a possible yield point. => NO libxtc errno
  request, NO errno-in-hook; instead AUDIT for the read-errno-across-yield
  anti-pattern and fix (read errno immediately after the raw syscall).

REVISED PLAN:
BUILD NOW (two-reviewer gate, while still pinned, process mode byte-for-byte):
 1. Lazy fiber-ctx hook: chain __xtc_fiber_ctx_save/restore to save/restore the 6
    roots + rebind `carrier` to the executing loop + bump per-fiber gen (fields
    re-derive on first touch via existing owner tokens).
 2. Route session GUC reads through the SESSION ROOT POINTER so the 230-entry GUC
    rebind vanishes (swap session root = 0.8ns; nothing to rebind). Confirm the GUC
    hashtable's session-vs-process layering roots cleanly.
AUDIT+FIX NOW (cheap): errno anti-pattern sweep (raw libc syscall whose errno is
 read after a yield point) + confirm postmaster SIGTERM/SIGINT + fault-guard compose
 with libxtc signal model.
REQUEST FROM LIBXTC (write-up to /tmp, user passes it on): expanded xtc_tls_opts /
 config-callback covering PG's full SSL GUC surface (cipher list + TLS1.3
 ciphersuites, CRL/CRL-dir, ECDH curve, passphrase callback ssl_passphrase_command,
 client-cert DN extraction, SSL_OP_NO_TICKET/NO_RENEGOTIATION/CIPHER_SERVER_PREFERENCE,
 session-cache, SNI). xtc_tls_* IS a real fiber-aware TLS stack (tls_openssl/mbedtls/
 gnutls/wolfssl/schannel backends) -> adopting it dissolves PG's per-OS-thread
 OpenSSL error-queue hazard entirely; migration stages behind the expanded opts.
 Current xtc_tls_opts only: cert/key/ca/verify_peer/alpn/min_version/max_version.
No unpin in the hook change. No big re-benchmark until a gated change warrants it.

### IMPLEMENTED: lazy fiber-ctx hook + GUC session-rooting (2026-07-20)

Landed (still pinned, process mode byte-for-byte, two adversarial reviews):

- LAZY fiber-ctx hook in src/backend/postmaster/pg_xtc_carrier.c (USE_XTC_CARRIER
  only). A per-fiber XtcPgFiberCtx blob lives on xtc_carrier_proc's fiber stack;
  installed via a chain onto libxtc's __xtc_fiber_ctx_save/restore in
  xtc_carrier_proc before any yield, and cleared at both fiber-exit sites.
  save() snapshots the 6 roots into the blob and RETURNS THE REAL PROC (chained
  proc-layer save result); restore() chains the proc-layer restore FIRST, then
  reinstalls the 6 roots via PgRuntimeRestoreCurrentWorkLazy IF the thread-local
  blob's recorded proc matches the resumed proc.
- CRITICAL FINDING (caused an intermittent crash before the fix): libxtc's
  __xtc_fiber_ctx_save/restore are PROCESS-GLOBAL and proc.c
  UNCONDITIONALLY resets them to __xtc_proc_ctx_save/restore on EVERY
  xtc_proc_spawn from ANY carrier thread. So a save-with-ours /
  restore-with-proc.c torn pair happens across a concurrent spawn. If save
  returned our blob, proc.c's restore (`__current_proc = ctx`) would set
  __current_proc to the blob -> __xtc_proc_kill_check derefs garbage ->
  fault-handler recursion (observed core: WalWriter/WaitIO fiber resume).
  FIX: our save returns the real proc, so BOTH restores set __current_proc
  correctly under the race; a lost restore-hook only costs the fiber-local
  root reinstall, which the manual wait-boundary seams already perform. This
  is why the hook is safe/neutral while pinned and why the manual seams stay.
- GUC session-rooting: no new plumbing needed. guc_variables / guc_variable_states
  / num_guc_variables / guc_hashtab are already #define'd to session-rooted
  accessors (PgCurrentGUCVariablesRef / PgCurrentGUCVariableStatesRef etc.,
  rooted in CurrentPgSession), and hot GUC reads (miscadmin.h work_mem, ...)
  use owner-token hot-field caches keyed on CurrentPgSession. So swapping the
  session root re-resolves all GUC reads lazily; the 230-entry
  RebindSessionGUCVariablePointers is NOT needed on a switch (only once per
  session-array at InitializeThreadedSessionGUCOptions / PgCarrierAttachBackend).
  PgRuntimeRestoreCurrentWorkLazy = PgRuntimeSetCurrentWork(...,false) skips it,
  matching the already-established PgSetCurrentSession(rebind=false) behavior.
- errno-across-yield AUDIT: clean, nothing to fix. Every backend-fiber yield
  site reads errno from a FRESH syscall after the yield (waiteventset.c
  epoll_wait(0) after the park; proc.c self-pipe drain read()) or sets errno
  from libxtc's converted return code (fd.c xtc_aio_f[data]sync errno=-xrc;
  method_xtc.c stores the negative-errno result). No raw libc errno is read
  across a yield.
- SIGNAL compose CONFIRMED (read-only, no change): postmaster keeps
  SIGINT/SIGTERM on its main thread (postmaster.c pqsignal + UnBlockSig
  ServerLoop); carrier bring-up blocks all signals around app-create + the raw
  scheduler pthread_create (pg_xtc_carrier.c BlockSig), and libxtc loop/pool
  threads start all-blocked via __os_pthread_create_masked; the R1 fault guard
  (xtc_fault_guard_install, PG_XTC_NO_FAULT_GUARD) is per-loop-thread. The hook
  touches no signal state and fires at the SIGVTALRM-preemption switch too via
  the same __xtc_fiber_ctx_* pointers. No conflict.
- ENV NOTE: this checkout's runtime RUNPATH/pkg-config leaks an old
  libxtc-1.8.0 ahead of the 1.23.3 the build links against
  (PKG_CONFIG_PATH_FOR_TARGET ends in xtc-1.8.0). The hook is stable under BOTH
  (verified 001 x6 green, pooled 007 x3, and a direct 1.23.3-forced repro).
  The intermittent pre-fix crash reproduced under both versions -- it was the
  global-reset race, not a version skew.
- Test: test_current_work_snapshot_lazy_restore (test_backend_runtime_carrier.c)
  exercises the save -> sibling-switch -> lazy-restore round trip and confirms
  the 6 roots + MyProc/MyLatch round-trip; GUC session-rooting is covered by the
  existing test_session_connection_guc_state_is_session_local (rebind=false swap).
- NO unpin. Correctness-neutral while pinned. Re-benchmark deferred to the later
  gated unpin stage.

### Post-hook sequence (2026-07-20): (a) independent review -> (b) unpin prereqs -> new libxtc (TLS+errno) -> benchmark

USER-APPROVED ORDER:
(a) INDEPENDENT adversarial review of d3a64636fad (the lazy fiber-ctx hook) --
    the in-session reviews were not truly independent (nested subagent tool was
    unavailable). Two parallel independent reviewers dispatched: chaining/race/
    lifetime, and GUC/lazy-restore/memory-coherence. Address any defect before (b).
(b) STAGE THE UNPIN PREREQUISITES (the no-steal guards + carrier re-resolution),
    but do NOT unpin yet in the same change -- unpin is the flip AFTER prereqs +
    review. Prereq targets (from the audit + the hook's own ponytail note):
    - CARRIER RE-RESOLUTION: the hook's restore currently trusts the SAVED carrier
      (valid only while pinned). For migration, re-resolve carrier from the loop
      the fiber actually resumed on (libxtc __xtc_current_loop / current-loop API).
      Marked ponytail in xtc_pg_fiber_ctx_restore (pg_xtc_carrier.c).
    - NO-STEAL GUARDS (a fiber holding these must NOT be migrated mid-section):
      (1) OpenSSL per-OS-thread error queue: guard the ERR_clear_error()...
          ERR_get_error() span in be-secure-openssl.c as no-yield/no-steal.
      (2) error-recovery sigprocmask(UnBlockSig) at postgres.c ~5493/~7065 --
          mark no-steal.
      (3) unpack_sql_state static buffer -- audit callers / make no-steal or
          per-fiber.
      (4) SPINLOCK HOLD: a fiber holding a raw slock_t (s_lock) must not be stolen
          (no yield point inside a spinlock section, so libxtc won't switch there
          -- CONFIRM libxtc only switches at yield points, i.e. a spinlock section
          contains no await/park; if confirmed this is free, else add a guard).
      Determine libxtc's "no-steal / pin-for-section" primitive (a critical-section
      / unsafe-bracket like __xtc_unsafe_enter/leave, or xtc_proc_recovery, or a
      per-fiber no-migrate flag) and use it. VERIFY-BEFORE-REQUESTING: if libxtc
      lacks a scoped no-steal primitive, write a /tmp request.
THEN: integrate a new libxtc with the TLS-opts expansion (per /tmp/
    libxtc-tls-opts-expansion-request.md) + any errno additions -> migrate
    be-secure-openssl to xtc_tls_* behind the expanded opts.
THEN (and only then): UNPIN backend fibers + re-run the A/B benchmarks (user gates
    the big run). Expect the 54%-idle/__xtc_exec_try_steal ceiling to move once
    work-stealing actually rebalances runnable fibers across idle carriers.

### libxtc TLS expansion REPLY received (2026-07-20) -- v1.24.0 (main b6d57a3)

/tmp/libxtc-tls-opts-expansion-reply.md: the libxtc team LANDED essentially all
our blockers + needed items for xtc_tls_* (ships v1.24.0). Assessment:
- Channel binding (#24) DONE: xtc_tls_get_server_cert_hash() RFC-5929 exact
  (MD5/SHA1->SHA256 else cert sig-alg digest), XTC_E_RANGE on small buffer. This
  was THE security must-have (else SCRAM channel binding would silently drop).
- Peer DN/CN (#21) DONE with embedded-NUL rejection (our CVE-2009-4034 note).
- verify_peer KEPT + verify_peer_mode tri-state added (ABI-safe; zeroed opts ==
  old behavior; PG sets XTC_TLS_VERIFY_REQUEST). Good call.
- Hardening #9/#12 (no-reneg, moving-write-buffer) + no-compression/no-tickets/
  session-cache-off made UNCONDITIONAL server defaults (no knob).
- Parity accessors all in (version/cipher/bits/issuer/serial/ALPN-selected,
  cipher_list/ciphersuites_13/groups, crl_file/crl_dir, passphrase_cb,
  prefer_server_ciphers). Non-OpenSSL backends return XTC_E_NOSYS/NULL (we're
  OpenSSL-only -> fine). RFC-5705 exporter correctly NOT added (PG uses only
  tls-server-end-point).

OPEN GATES to verify BEFORE the be_tls_* -> xtc_tls_* port (do NOT assume):
1. SNI (#29) is DEFERRED -- "blocker only when ssl_sni=on". CONFIRM PG's ssl_sni
   default + whether our threaded config enables it. If server-side SNI matters,
   either gate ssl_sni=off for threaded-TLS-unpin, accept it, or ask libxtc to
   prioritize #29. This is the one remaining TLS-unpin gate.
2. Confirm PG's SSL posture matches the UNCONDITIONAL hardening defaults (no PG
   GUC/extension path expects to toggle no-reneg/compression/tickets/cache). If
   identical + non-configurable -> strictly better; if PG toggles any -> subtle
   behavior change to gate.

SEQUENCE UNCHANGED: this is the THIRD step (integrate new libxtc TLS+errno). It
comes AFTER (a) hook review (in-flight) + (b) unpin prereqs. The reply de-risks
the TLS-fiber unpin gate (now open modulo SNI) but does NOT change the current
step. Bump libxtc pin to v1.24.0 (fresh build dir) when we reach the TLS-port
step, then port be_tls_* onto xtc_tls_* behind the expanded opts.

### ssl_sni no-migrate invariant + errno-audit closure (2026-07-20) -- v1.24.0 pinned

Scope decision for the TLS/errno adoption step (be_tls_* -> xtc_tls_*):

WHAT LANDED (gated, process + non-fiber-threaded byte-for-byte, no unpin):
- ssl_sni no-migrate INVARIANT tripwire. be_tls_open_server() now asserts,
  under USE_XTC_CARRIER only, that a backend fiber is NOT simultaneously
  (a) using server-side SNI (ssl_sni=on) and (b) migratable across carriers.
  libxtc v1.24 DEFERRED #29 (the ClientHello context-swap), so a migrated
  ssl_sni connection could not keep per-host cert/CA selection under the
  fiber-aware stack -- a correctness/security regression. ssl_sni defaults to
  off (guc_parameters.dat boot_val=false), so the common threaded-TLS path is
  unaffected; the assert names the exact wrong assumption a future unpin must
  not make. New carrier predicate xtc_pg_backend_fiber_is_migratable() returns
  false unconditionally (fibers are pinned in this build); it is the single
  point the gated unpin flips to a real per-fiber query, at which point the
  invariant becomes live rather than dead. In a non-assert build and in process
  mode the guard compiles to nothing.
- errno adoption/AUDIT: COMPLETE and CLEAN (re-verified fresh on this tree).
  * No v1.24 errno API to adopt: grep of the v1.24 headers finds errno only as
    a return-value convention ("negative errno"); no xtc errno getter/setter.
    errno stays libxtc-internal (XTC_E_* space, captured pre-yield, EINTR/EAGAIN
    handled internally). Nothing to wire.
  * No read-errno-across-yield anti-pattern in carrier/backend-fiber-reachable
    code. Every yield site reads errno from a FRESH syscall after the resume
    (waiteventset.c: xtc_pg_wait_fd() park -> epoll_wait(...,0) -> errno of that
    call; self-pipe drain read()), or sets errno from libxtc's converted
    negative return (fd.c xtc_aio_f[data]sync errno=-xrc; method_xtc.c). The
    TLS retry loop (be-secure.c) reads errno set by be_tls_read/write BEFORE the
    WaitEventSetWait yield and re-runs a fresh be_tls_* on retry; be-secure-
    openssl.c reads errno immediately after SSL_*/the non-blocking BIO recv/send.

CHANNEL-BINDING EQUIVALENCE (tls-server-end-point, RFC 5929) -- proven by spec:
  PG's be_tls_get_certificate_hash() = X509_get_signature_info() -> sig NID ->
  {MD5,SHA1 => SHA-256 ; else EVP_get_digestbynid(nid)} -> X509_digest().
  libxtc's xtc_tls_get_server_cert_hash() documents (and its OpenSSL backend
  implements) the identical RFC-5929 rule via the same X509_get_signature_info
  -> X509_digest path, with XTC_E_RANGE on a too-small buffer. A standalone
  reference run of PG's exact algorithm on the ssl test certs yields the same
  32-byte SHA-256 digests libxtc would produce for the same cert bytes
  (server-cn-only sha256WithRSA and server-rsapss rsassaPss both resolve to
  SHA-256). So the migration preserves channel binding byte-for-byte when the
  live swap lands. NOTE: this is a SPEC/algorithm equivalence proof; end-to-end
  byte-equality across a live xtc_tls handshake is validated by libxtc's own
  test_tls_server case and would be re-validated by the ssl/002_scram suite once
  a threaded ssl harness runs the fiber-aware path.

WHAT IS STAGED (deliberately NOT landed this pass -- defer with invariant):
  The RUNTIME be_tls_* -> xtc_tls_* backend swap is NOT landed. Rationale:
  (1) No forcing function: TLS-bearing backend fibers are PINNED (no unpin in
      this task), so the existing directly-driven-OpenSSL be_tls_* path is
      correct and secure under the carrier today (a pinned fiber never reads a
      foreign thread's OpenSSL error queue). The xtc_tls_* swap ENABLES a future
      TLS-fiber unpin; it is not a fix for current behavior.
  (2) Cannot be validated security-equivalent under the constraints: the
      threaded TAP harness (THREADED_WORLD_CORE_TAP_TESTS) does NOT run the
      src/test/ssl suite, so a live threaded-mode swap has no cert-verification/
      CRL/channel-binding/SNI regression coverage. The ssl suite runs only in
      process mode. Landing a parallel production TLS backend without that
      coverage is exactly the "half-wired path could serve a connection with
      weaker security" the task forbids.
  (3) The swap is a ~2500-line security-critical rewrite (custom BIO, per-host
      SNI contexts, DH/ECDH, verify_cb with cert_errdetail chaining, ALPN,
      passphrase, and the Port struct's typed SSL*/X509* fields consumed by
      auth-scram/backend_status/postinit). It should land as its own coherent,
      independently-reviewed change WITH a threaded ssl harness, immediately
      before or with the TLS-fiber unpin (plan step (b)/unpin), not speculatively
      ahead of the forcing function.
  The be_tls_* -> xtc_tls_* mapping is fully specced in the v1.24 reply and this
  section; the accessors (version/cipher/bits/subject-dn/issuer-dn/serial/
  server-cert-hash/has-peer-cert/alpn-selected) and opts (verify_peer_mode=
  XTC_TLS_VERIFY_REQUEST, cipher_list, ciphersuites_13, groups, crl_file/dir,
  prefer_server_ciphers, passphrase_cb<-ssl_passphrase_command) are all present
  and confirmed in the v1.24 headers/lib the meson build links (xtc-1.24.0).

OPEN GATE 2 (hardening posture) RESOLVED: PG's SSL_OP_NO_RENEGOTIATION /
  NO_COMPRESSION / NO_TICKET / SESS_CACHE_OFF / MODE_ACCEPT_MOVING_WRITE_BUFFER
  are all hardcoded (no GUC toggles) in be-secure-openssl.c, matching libxtc's
  unconditional server defaults exactly -> strictly-equivalent, no behavior gate.

VALIDATION (this pass): meson build 0 warnings; test_backend_runtime 12 OK / 0
  Fail / 2 SKIP (001=128/128, 003=43/43, 007=46/46); process regress 245/245;
  full src/test/ssl PG_TEST_EXTRA=ssl 001/002_scram/003/004_sni all green
  (272/28/21/102) -- confirms the OpenSSL path is byte-for-byte unchanged and
  channel binding + SNI still work in process mode.

### UNPIN BLOCKED: pinning is inside libxtc's coro layer, no opt-out API (2026-07-20)

Attempted "make fibers no longer pinned"; VERIFIED it is NOT possible from the
PG side (verify-before-requesting changed the answer again):
- Every coroutine is spawned pinned=1 HARDCODED at the coro layer: coro_fctx.c:387
  __xtc_task_spawn_ex(loop, __xtc_coro_step, c, 1, &t) (literal 1; also
  coro_uctx.c:622, coro_winfiber.c:121). An xtc_proc IS a coroutine -> every PG
  backend fiber is pinned at creation.
- xtc_proc_opts_t (xtc_proc.h:72) has NO pinned/migratable field.
- The stealable deque path (loop.c:310 `if (!t->pinned && xtc_deque_push...)`) is
  only reachable via raw __xtc_task_spawn_ex(...,pinned=0) (task.c:194) which the
  proc/coro layer never uses. No API unpins a live proc or spawns one migratable.
=> Flipping xtc_pg_backend_fiber_is_migratable() to true would be a LIE: libxtc
   would still never steal the fiber, but our invariants/guards would arm for a
   migration that can't happen -> corruption risk. NOT DONE.
=> REQUEST written: /tmp/libxtc-migratable-proc-request.md -- (1) an opt-in
   xtc_proc_opts_t.migratable (threads pinned=0 down; default unchanged), (2)
   confirm the scoped no-steal/resume-on-same-loop guarantee of
   __xtc_unsafe_enter/leave + xtc_proc_critical_enter/leave (preempt_int.h/
   ptc_ext.h ALREADY EXIST for lock-holders) for a MIGRATABLE proc, (3) placement
   policy to avoid migration thrash. Everything PG-side (fiber-ctx hook, flip
   point, guard sites) is staged waiting on this API.

NO-STEAL PRIMITIVE (found, for the guards): __xtc_unsafe_enter/leave (defers
involuntary SIGVTALRM preemption + protects lock-holders) and
xtc_proc_critical_enter/leave. The open question for the request: do these
guarantee resume-on-SAME-loop across a voluntary park for a migratable proc, or
is a separate pin-for-section needed? (A running non-parked fiber is presumably
never moved mid-instruction since stealing is at enqueue of a parked/runnable
task -- to be confirmed by libxtc.)

TLS integration (be_tls_* -> xtc_tls_*) is INDEPENDENT of unpin (only enables
TLS-connection migration later) -> proceeding NOW as its own gated change with a
threaded ssl harness.

### DECISION: transition backend workers from xtc_proc to pure fibers (2026-07-20)

USER DIRECTION: procs don't migrate, fibers do -> transition backend WORKERS to
pure fibers (xtc_async/xtc_task coro surface, spawned pinned=0 = stealable), use
libxtc SUPERVISOR features to monitor them; libxtc provides crash containment
for fibers. FULLY INVESTIGATE + DESIGN using libxtc features BEFORE converting.
(This supersedes /tmp/libxtc-migratable-proc-request.md -- no new libxtc API is
needed; the migratable path already exists, we were just on the proc side.)

VERIFIED libxtc surfaces so far:
- xtc_async (L2, xtc_async.h): stackful fiber via ucontext, xtc_await recovers
  return value. Spawned via xtc_task_spawn -> pinned=0 (task.c:194) = STEALABLE.
  xtc_exec_spawn (exec.c:896) also pinned=0; xtc_exec_spawn_on pins.
- xtc_orc (L4, xtc_orc.h): supervisor with ONE_FOR_ONE + restart intensity/policy
  -- BUT the supervisor is itself an xtc_proc and xtc_child_spec.fn is an
  xtc_proc_fn; it acts on child DOWN messages (proc mechanism).
- xtc_svr (L4 gen_server): also runs as an xtc_proc.
- Fault containment + DOWN are today L3 xtc_proc features (key on __current_proc).
  User states libxtc provides fiber crash containment -> the design MUST locate
  the fiber-level containment mechanism (recovery frame per coro? auto-armed?)
  and the fiber-level exit/crash notification the supervisor observes.

WHAT WE LOSE dropping xtc_proc for workers (must be re-provided via fiber
features): (1) fault containment SIGSEGV->contained->attributed crash; (2) exit
classification CLEAN/EXIT/SIGNAL/NOPROC (supervisor DOWN) -> likely xtc_await +
an explicit fiber status; (3) xtc_self/pid identity (fiber-ctx hook owner-guard,
spawn/signal path) -> re-key on xtc_task_t*; (4) mailbox (if used). Carrier +
supervisor + crash-escalation (ExitPostmaster) are built on proc DOWN today
(pg_xtc_carrier.c XTC_DOWN_KIND_*, spawn_monitor).

DESIGN QUESTIONS to answer from source BEFORE coding:
 - Does a pure xtc_async/xtc_task fiber get SIGSEGV fault containment (recovery
   frame), or is proc-hood required? If fibers are contained, HOW is a crash
   reported to a watcher (vs proc DOWN)?
 - Can a supervisor observe fiber exit/crash without DOWN (xtc_await from a
   supervisor fiber? a task-completion callback? a status the fiber sets)?
 - Keep the SUPERVISOR as a proc watching fiber workers, or make everything
   fibers? What does xtc_orc require? Does a proc-supervisor watching pinned=0
   fiber workers even work (child_spec.fn is xtc_proc_fn)?
 - The fiber-ctx hook (50434faae9f) currently chains proc-layer __current_proc
   preservation and owner-guards on xtc_self/pid -- redesign for a fiber worker
   (task handle identity; does the coro layer still call the ctx hooks for a
   pure fiber? -- coro_fctx.c calls them, so YES).
 - Crash escalation to ExitPostmaster must be preserved (a genuine backend fault
   MUST escalate in a shared-process model).
 - Preserve process mode + non-fiber threaded mode byte-for-byte; workers-as-
   fibers only under USE_XTC_CARRIER threaded carrier.

DELIVERABLE: a design doc (plan_docs/MULTITHREADED_FIBER_WORKER_DESIGN.md) with
the exact libxtc APIs, the containment+notification mechanism, the supervisor
shape, the identity re-keying, the fiber-ctx-hook redesign, the incremental
conversion plan (smallest gated steps, each two-reviewer + green), and the
migration-safety implications (once workers are stealable fibers, the no-steal
guards for spinlock/OpenSSL-ERR spans become LIVE -- so the guard work couples
in). NO conversion code this step -- design first, then we execute in gated
increments.

### Threaded ssl harness LANDED; be_tls_* -> xtc_tls_* swap STAYS gated on a runtime-lib blocker (2026-07-20)

Built the threaded ssl test harness the a75bcc60b3b deferral said was the
missing prerequisite for validating the be_tls_* -> xtc_tls_* swap.  With the
harness in place the swap was re-evaluated against runtime evidence and stays
DEFERRED (gate CLOSED) -- now for a CONCRETE, newly-found blocker, not just the
absence of a forcing function.

WHAT LANDED (test infrastructure only; ZERO backend C changes -> process +
threaded byte-for-byte, no security surface touched):
- src/test/ssl/threaded_ssl.conf: a TEMP_CONFIG that boots the ssl TAP suite
  under the multithreaded carrier (multithreaded=on, thread-per-session,
  io_method=sync).  PostgreSQL::Test::Cluster appends TEMP_CONFIG to
  postgresql.conf, so the UNMODIFIED ssl tests run against a threaded server.
- check-threaded-ssl target (src/test/ssl/Makefile + GNUmakefile.in): runs
  001_ssltests + 002_scram + 004_sni under threaded_ssl.conf.  It exercises,
  ON THE CARRIER BACKEND-FIBER PATH: server-cert handshake, client-cert auth
  (peer DN/CN), CRL reject, SCRAM channel binding (tls-server-end-point,
  channel_binding=require), and confirms ssl_sni handling.  003_sslinfo is
  excluded: the bundled sslinfo contrib module is process-backend-only (no
  PG_MODULE_MAGIC_EXT threaded backend-model metadata), so the threaded runtime
  correctly REFUSES to load sslinfo.so ("library ... is not supported in the
  threaded backend runtime").  Marking sslinfo threaded-safe is Phase 16 /
  Gate E2-Extensions work, not TLS work; 001/002/004 already cover the full
  server-side TLS security surface on the carrier.

HARNESS VALIDATION (carrier confirmed up -- "xtc: carrier scheduler thread up
(8 loops, 8 supervisors)" in the node log, 26 SSL connections authorized):
  threaded 001_ssltests 272/272, 002_scram 28/28, 004_sni 102/102 (402
  subtests).  LIVE channel binding: 002_scram's channel_binding=require SCRAM
  handshakes succeed over a TLS connection served by a backend fiber -- the
  client's tls-server-end-point hash matches the server's, proving the harness
  exercises channel binding under the carrier.  (Today that hash still comes
  from PG's OpenSSL be_tls_get_certificate_hash; when the xtc_tls swap lands,
  THIS SAME TEST re-validates xtc_tls_get_server_cert_hash byte-equivalence
  live.)  Process-mode ssl unchanged (272/28/21/102), test_backend_runtime 12
  OK / 0 Fail (001=128, 003=43, 007=46), process regress 245/245.

WHY THE SWAP STAYS GATED (runtime evidence, not assumption):
1. RUNTIME LIBXTC LACKS THE SECURITY ACCESSORS.  The build COMPILES against
   xtc-1.24.0 (20 xtc_tls_* symbols incl. get_server_cert_hash /
   get_peer_subject_dn / has_peer_cert / the CRL+verify_mode+passphrase opts),
   but the postgres binary's RUNPATH lists xtc-1.8.0 BEFORE xtc-1.24.0, so the
   loader binds libxtc.so.1 -> 1.8.0 at runtime (confirmed: ldd + patchelf
   --print-rpath).  1.8.0 exports only 10 xtc_tls_* symbols and is MISSING
   every v1.24 security accessor.  A swap built on 1.24 headers but running
   against 1.8 would either fail to resolve the new symbols OR -- worse --
   pass the larger v1.24 xtc_tls_opts_t to 1.8's smaller ctx_create, whose
   struct has no crl_file/crl_dir/verify_peer_mode/passphrase_cb fields, so
   CRL checking, client-cert verification, and channel binding would be
   SILENTLY DROPPED.  That is a CVE-class weaker TLS path -- exactly what the
   task forbids.  Unblocking requires fixing the RUNPATH order (prefer 1.24.0)
   in the nix-config packaging, OUTSIDE this source tree.  PG's own TLS is
   unaffected: be_tls_* uses PG's directly-linked OpenSSL (libssl.so.3), not
   libxtc, so the harness results are valid regardless of the libxtc leak.
2. NO FORCING FUNCTION.  TLS-bearing backend fibers are PINNED (unpin BLOCKED
   on the libxtc migratable-proc API request -- see "UNPIN BLOCKED").  The swap
   ENABLES a future TLS-fiber unpin (state lives in xtc_tls_t, not the
   per-OS-thread OpenSSL error queue); it fixes nothing today, because a pinned
   fiber never reads a foreign thread's error queue.
3. NO PARTIAL SAFE INCREMENT.  A connection either uses OpenSSL be_tls_* (today)
   or xtc_tls_* (fully) -- the handshake + I/O boundary + all accessors move as
   one unit.  There is no half that serves a connection identically AND is
   independently testable; a half-wired path is the forbidden weaker path.

SO: the swap co-lands with the TLS-fiber unpin, AFTER (a) the libxtc
migratable-proc API lands (unblocks unpin) and (b) the RUNPATH prefers 1.24.0
(so the security accessors are actually present at runtime).  At that point the
check-threaded-ssl harness is the live regression gate: it must stay green with
the xtc_tls path serving the carrier connections, and 002_scram re-proves
channel-binding byte-equivalence end-to-end.  The a75bcc60b3b ssl_sni
no-migrate invariant and the be_tls_* -> xtc_tls_* mapping spec remain the
land-time checklist.

### RUNTIME-LIB BUG CAUGHT + FIXED: postgres was binding stale libxtc 1.8.0 (2026-07-20)

The threaded-ssl agent (04d79b80adc) caught a serious latent bug: the built
postgres binary's RUNPATH listed .../xtc-1.8.0/lib BEFORE .../xtc-1.24.0/lib, so
the loader bound libxtc.so.1 -> 1.8.0 at RUNTIME even though we COMPILE against
1.24.0 headers. So all prior "v1.24.0" validation this session actually exercised
1.8.0 (tests passed only because the 1.8/1.24 ABI overlaps for what we use). This
would have SILENTLY undermined the fiber-worker transition (which depends on 1.24
migratable-spawn / fiber-containment semantics) and was exactly why the TLS swap
had to stay gated (a v1.24 xtc_tls_opts_t passed to 1.8's ctx_create = short-
struct read -> dropped CRL/client-cert-verify/channel-binding = CVE-class).

ROOT CAUSE: a stale RUNPATH baked into the postgres binary + tmp_install copy from
an incremental link that PREDATED this session's 1.24.0 bump; the fresh `meson
setup` did not force a relink of the already-linked postgres target. The flake
(flake.nix) correctly pins ONLY 1.24.0 (e944d00) -- no 1.8.0 reference -- so this
is a stale-artifact issue, NOT a nix-config bug.

FIX: forced a clean relink (touch + ninja postgres) + full `ninja -j2` +
`meson install`. Verified: RUNPATH now has ONLY xtc-1.24.0; ldd resolves
libxtc.so.1 -> 1.24.0 for postgres, the installed copy, and test .so's; no 1.8.0
RUNPATH remains in any built artifact (only stale meson-logs text). Re-ran the
threaded gate against GENUINE 1.24.0: test_backend_runtime 15 OK / 0 Fail / 2
SKIP (001=128/128, 003=43/43, 007=46/46). Process regress previously 245/245.

LESSON (add to build hygiene): after a libxtc pin bump, a fresh `meson setup` is
NOT sufficient -- force a relink (or fully fresh build tree) and VERIFY
`ldd build/src/backend/postgres | grep xtc` resolves the intended version before
trusting any runtime validation. The threaded-ssl harness (check-threaded-ssl,
src/test/ssl/threaded_ssl.conf) is the live TLS gate for when the swap lands
(after the fiber-worker unpin + confirmed 1.24 runtime binding).

### DESIGN LOCKED: xtc_proc workers with pinned=0 (stealable coros) (2026-07-20)

The fiber-worker investigation (plan_docs/MULTITHREADED_FIBER_WORKER_DESIGN.md)
proved libxtc containment+supervision are PROC-KEYED (a bare xtc_async fiber:
SIGSEGV -> process killed, no DOWN, xtc_self=NOPROC; an xtc_proc: contained ->
DOWN(signal), survives). A proc IS already a coroutine (xtc_proc_spawn ->
xtc_async(__proc_entry)); pinning is an L2 coro property (pinned on the task),
orthogonal to L3 proc-hood, coupled only by xtc_async hardcoding pinned=1.

LOCKED MODEL: keep xtc_proc workers (identity/pid, recovery/containment, DOWN
supervision, mailbox) and make their underlying coro pinned=0 (STEALABLE). This
is the IDIOMATIC libxtc/BEAM model: supervised process with identity that the
scheduler is free to migrate across carriers for load balancing. Bare fibers are
the UN-idiomatic choice here (anonymous, unsupervised, uncontained -- libxtc's own
xtc_launch wraps "per-fiber recovery" in a proc). Migration-safety price is SMALL:
a running proc is never moved mid-instruction (stealing pops SCHEDULED-not-running
deque tasks only); every thread-affine site (spinlock, OpenSSL ERR span,
sigprocmask, static scratch) is yield-free -> assert-tripwires suffice, no park-pin.

ONE verified-absent libxtc piece: xtc_proc_opts_t.migratable (thread a pinned arg
to the xtc_async the proc spawn already calls; default off = today, ABI-neutral).
Request: /tmp/libxtc-migratable-proc-opts-request.md. (Supersedes the earlier
migratable-proc request framing -- now precise: it's a proc OPTS field, not a new
proc-vs-fiber API.)

PHASED PLAN (each two-reviewer + 0-warnings + 0-Fail + 245/245 green):
 A) carrier re-resolution in fiber-ctx hook (re-resolve carrier root from
    __xtc_current_loop on resume, not the saved value) -- PINNED, neutral.
 B) no-steal assert-tripwire scaffolding at the affine sites -- PINNED, dead.
 C) libxtc migratable opt-in wired (default off, ABI-neutral) -- gated on the
    libxtc request landing.
 D) FLIP: spawn backend-worker coros pinned=0 -> migration goes LIVE (needs A+B+C).
 E) wake-nudge accuracy (measured). F) benchmark (user gates).
A+B have NO dependency on the libxtc flag -> landing NOW so D is a small gated flip.

### Phase A+B landed + two premise corrections (2026-07-20)

Commits: Phase A 1b102560377, Phase B 75f6d32c469 (both pushed).

CORRECTION 1 (the stale-1.8.0 binding ROOT CAUSE -- supersedes the "stale
artifact" theory in 6cc4df0f4af): the Nix dev-shell injects -L.../xtc-1.8.0/lib
into BOTH NIX_LDFLAGS and NIX_LDFLAGS_FOR_TARGET (cc-wrapper reads both) -> 1.8.0
lands in postgres RUNPATH BEFORE 1.24.0. build.ninja only ever references 1.24.0;
the 1.8.0 comes purely from the wrapper's -L->rpath translation. So it RECURS on
a fresh build, it is NOT a one-time stale artifact. BUILD HYGIENE (mandatory
before trusting any runtime validation): scrub xtc-1.8.0 from NIX_LDFLAGS AND
NIX_LDFLAGS_FOR_TARGET, force a relink, and verify `ldd build/src/backend/postgres
| grep xtc` -> xtc-1.24.0. (Real fix belongs in nix-config: drop the 1.8.0 -L
from the dev shell. Tracked as a nix-config follow-up, different repo.)

CORRECTION 2 (Phase A -- the design's per-loop-carrier premise was INVERTED):
in the xtc fiber-per-session model the PgCarrier is FIBER-OWNED (embedded in
BackendThreadStart.runtime_state.carrier, launch_backend.c:300; current_backend/
session/execution point at that fiber's logical state), so it travels WITH the
fiber across a steal -> snap.carrier is ALREADY correct after migration; there is
NO per-loop PgCarrier to re-resolve from (xtc_exec_loop returns xtc_loop_t*, not
PgCarrier). Even the ostensibly thread-affine carrier fields are fiber-owned
(self-pipe per fiber; stack_base is the fiber's own migrating coro stack). GOOD
NEWS: one fewer migration-safety concern. Phase A landed the CORRECT neutral form
-- an assert-only tripwire that confirms the resumed loop (xtc_exec_loop_id(),
reads __xtc_current_loop) == the fiber's spawn loop while pinned (dead), turning a
future wrong-loop resume into a loud failure -- instead of a wrong root swap.

PHASE B: __thread affine-section depth (USE_ASSERT_CHECKING only) with park-
boundary Assert(is_migratable() || depth==0) at the fiber-ctx SAVE hook and
xtc_pg_wait_fd; brackets at the OpenSSL ERR span (be_tls_read/write) and the
sigprocmask recovery windows (postgres.c ~5493/7065); spinlock + unpack_sql_state
documented safe-by-construction + enforced transitively. Dead while pinned
(proven under a cassert build: A/B tripwires fired 0 times). Release + process
byte-for-byte (Assert discards its arg in release; XtcPgNoStealEnter/Leave =
((void)0)).

VALIDATION: 0 warnings; ldd 1.24.0; test_backend_runtime 0 Fail (001=128/128,
003=43/43, 007=46/46); process regress 245/245; cassert deadness proven.

OPEN: (a) truly-independent adversarial review of 1b102560377 + 75f6d32c469
STILL REQUIRED (sub-agent ran self-passes only). (b) Pre-existing cassert
assertion guc.c:1363 (+ lwlock.c:1214) in the threaded snapshot test
(test_current_work_snapshot_lazy_restore) under USE_ASSERT_CHECKING -- NOT caused
by A/B, does not affect the release gate; worth a separate look (known
cassert+threaded GUC memory-context issue).

NEXT: independent review of A+B; then Phase C (wire migratable, gated on the
libxtc opts request /tmp/libxtc-migratable-proc-opts-request.md); then D (flip
pinned=0, migration live); E wake-nudge; F benchmark.

### Phase A+B independent review CLOSED (2026-07-20)

Two truly-independent adversarial reviewers (not the sub-agent self-passes):
- Phase A (1b102560377): SHIP, no changes. Verified the load-bearing
  fiber-owned-carrier claim field-by-field against PG + libxtc source
  (current_backend/session/execution, self-pipe fds=process-global,
  signalfd=process-scoped, stack_base_ptr=fiber's own migrating coro stack,
  scheduler_execution, guc mutex depths -- ALL fiber-carried or process-global,
  none per-OS-thread). xtc_exec_loop_id() reads only __xtc_current_loop (safe in
  restore); loop_id==exec_id+1 bit-identical to libxtc proc.c:1130; provably dead
  while pinned; release/process byte-for-byte. Confirms nothing latent for Phase D
  from the carrier root.
- Phase B (75f6d32c469): HOLD -> 2 real defects (sigprocmask brackets wrapped
  PgSessionRecoverError which PARKS via pq_flush to a backpressured client and
  can ereport(FATAL) on lost protocol sync; per-OS-thread depth never reset ->
  spurious assert or leaked depth tripping the next reused-carrier fiber).
  FIXED in 5d8a4bb1b99: narrowed brackets to the sigprocmask() call only (Leave
  before RecoverError, both sites) -> structurally eliminates any parking/longjmp
  inside a bracket; crash-safe xtc_pg_affine_section_reset() at fiber entry + at-
  exit; test-coverage item moot by construction. Re-validated: 0 warnings, ldd
  1.24.0, cassert -Werror clean, test_backend_runtime 15 OK/0 Fail/2 SKIP, process
  regress 245/245.

STATUS: Phases A+B settled (reviewed, fixed, green). Migration-safety groundwork
solid; the fiber-owned-carrier premise (Phase D-critical) is independently
confirmed. Phase C/D BLOCKED on libxtc shipping xtc_proc_opts_t.migratable
(request: /tmp/libxtc-migratable-proc-opts-request.md). Pending non-blocking
items: independent review of the TLS-harness commit 04d79b80adc (test-infra+docs,
low risk); pre-existing cassert guc.c:1363 assertion in the threaded snapshot
test (not A/B-caused, does not affect release gate).

### RUNPATH 1.8.0 leak: RESOLVED -- was a stale shell, not a nix-config bug (2026-07-20)

Investigated the "recurs on every build" claim (Correction 1). In a FRESH
`nix develop` shell now: NIX_LDFLAGS/NIX_LDFLAGS_FOR_TARGET contain ONLY
xtc-1.24.0 (no 1.8.0); a clean relink produces RUNPATH with only xtc-1.24.0;
ldd -> 1.24.0. Nothing in the store references the 1.8.0 path from this flake;
outputs/out/lib has no xtc. => The 1.8.0 -L came from a STALE shell instance the
earlier agents had entered before the flake was re-evaluated against the 1.24.0
pin (the flake correctly pins only 1.24.0). NOT a persistent nix-config bug ->
NO nix-config change needed. The permanent fix already happened (flake update +
fresh shell). Operational lesson stands: after a libxtc pin bump, start a FRESH
nix develop shell and verify `ldd build/src/backend/postgres | grep xtc` before
trusting runtime validation; a long-lived shell can hold the old -L.

### TLS-harness commit 04d79b80adc independent review CLOSED: SHIP (2026-07-20)

Independent adversarial review (the required non-self pass): SHIP, no defects.
Verified against source: (1) ZERO backend C changes (4 files: GNUmakefile.in,
ssl/Makefile, threaded_ssl.conf, plan doc) -> process+threaded byte-for-byte;
(2) fail-loud PG_TEST_EXTRA guard exits 1 before prove, no skip->rc0 false-green,
all edge cases verified; (3) NO false-green path -- multithreaded=on reaches
postgresql.conf via TEMP_CONFIG, SSL handshakes genuinely run on carrier fibers,
io_method=sync is a RESPECTED explicit choice (not a degenerate process fallback);
(4) additive (default ssl check untouched); (5) 003_sslinfo exclusion is honest
Phase-16 scope (process-only PG_MODULE_MAGIC_EXT, genuinely refused threaded), no
hidden gap; (6) build-system hygiene sound, `make check-threaded-ssl
PG_TEST_EXTRA=ssl` works from clean tree. Nuance (not a defect): guard is stricter
than the test regex on comma-separated PG_TEST_EXTRA but errs fail-loud (safe).
check-threaded-ssl is the live TLS gate for when the be_tls_*->xtc_tls_* swap
lands (after Phase D unpin + confirmed 1.24 binding, both now in hand).

### libxtc v1.25.0 review: migratable flag LANDED but ABI narrowing HIDES our ctx-hook symbols (2026-07-20)

v1.25.0 (tag 913f3c49 / commit 8fe0155) has 4 changes vs v1.24.0 (e944d00):
- e7c3af0 feat(proc): xtc_proc_opts_t.migratable -- EXACTLY our request. pinned =
  !migratable threaded to __xtc_async_ex; default 0 = byte-identical (pinned).
  xtc_proc_spawn/_link/_monitor honor it. This unblocks Phase C/D in principle.
- 23242a7 build(abi): narrow the .so export to `global: xtc_*` + a 4-symbol
  allowlist (__xtc_proc_recovery_slot/_recovery_prep/_recovery_ctx/_recovery_result
  -- the only internals a public macro expands to); `local: *` hides EVERYTHING
  else, INCLUDING __xtc_fiber_ctx_save / __xtc_fiber_ctx_restore.
- f25d061 moved internal __ decls out of installed headers into *_int.h.
- 9564ca8 os_errno doc.

BLOCKER for a straight bump: OUR fiber-ctx hook (pg_xtc_carrier.c:597-598,831-847,
commit 50434faae9f) declares + reads + ASSIGNS the extern globals
__xtc_fiber_ctx_save / __xtc_fiber_ctx_restore to chain PG's 6-root save/restore
across a coro switch. We link libxtc SHARED (libxtc.so.1, -lxtc). Under v1.25.0's
version-script those __xtc_* symbols are now `local` (unexported) -> our postgres
will NOT resolve them against the .so. The migration ENABLER we built breaks
against the release that ships the migratable FLAG we need.

DESIGN SIGNAL (not just a link error): xtc_proc.h:602 explicitly documents the
ctx save/restore (__xtc_proc_ctx_save/restore) as "library-internal (the __
prefix)". The migratable feature preserves the PROC's __current_proc across
migration INTERNALLY; libxtc's stance is that consumers should NOT reach into the
fiber-ctx hook globals. Our hook was always reaching into internals; v1.25.0 makes
that explicit by hiding them.

OPTIONS (need a decision + likely a libxtc conversation; do NOT force-export the
symbols -- that fights an explicit ABI decision and rebreaks):
 1. Link libxtc STATIC (.a) instead of shared -- the version-script `local:` only
    hides from the .so; a static link still sees __xtc_fiber_ctx_save/restore.
    Quick unblock, but keeps us depending on symbols libxtc declared internal (a
    fragile long-term coupling; a future refactor could remove them).
 2. Ask libxtc for a PUBLIC per-fiber-switch consumer hook (or public per-proc
    storage that survives migration) so PG's 6 roots ride migration via a
    supported API instead of the internal ctx globals. The RIGHT long-term fix;
    aligns with "embrace the libxtc model." Verify-before-requesting: confirm no
    existing public mechanism (no xtc_proc_set/get userdata found; no public
    switch hook found).
 3. Re-examine whether PG's 6 roots even NEED a switch hook once the proc carries
    them: the fiber-owned-carrier finding (Phase A review) showed the 6 roots are
    fiber-owned and travel with the proc's stack; the hook's job is to repoint the
    THREAD-LOCAL bridge cells on resume. If those cells can instead be derived
    on-demand from proc-carried state (no thread-local cache needing a switch-time
    refresh), the ctx hook may be UNNECESSARY -- eliminating the dependency
    entirely. This is the most "embrace-the-model" answer and worth investigating
    before requesting a libxtc API.

STAY on v1.24.0 for now (green, hook works). Do NOT bump to v1.25.0 until the
ctx-hook dependency is resolved (option 1/2/3). The migratable flag is in hand the
moment we do.

### Option 3 VERDICT: eliminate the fiber-ctx hook -> bump to v1.25.0 cleanly (2026-07-20)

Design: plan_docs/MULTITHREADED_FIBERCTX_ELIMINATION_DESIGN.md. Verdict: the
fiber-ctx hook CAN be eliminated -- a PURE DELETION for the pinned world, zero
per-read cost, and v1.25.0 then links cleanly (no dependency on the now-hidden
__xtc_fiber_ctx_save/restore).

WHY IT'S A PURE DELETION (source-verified):
- The hook's only job is to repoint the thread-local bridge cells to the resuming
  fiber's 6 roots (pg_xtc_carrier.c:597-598 externs, 831-847 assign, restore via
  PgRuntimeRestoreCurrentWorkLazy). The manual wait-boundary SEAMS ALREADY do that
  repoint on every cooperative yield a backend fiber has: xtc_pg_wait_fd
  (PgRuntimeSaveCurrentWork/RestoreCurrentWork around the park, verified
  ~1248-1266), method_xtc.c:131/150/154, fd.c:543-545/602-604. Seams run
  unconditionally and are the documented standing correctness guarantee.
- The hook is explicitly "no-op-equivalent while pinned" (pg_xtc_carrier.c:576-587,
  verified). Its ONLY unique coverage is involuntary SIGVTALRM-preemption switches
  -- irrelevant while pinned, and the migration case handled at Phase D.
- Bridge derived fields self-invalidate: the owner token IS the root pointer
  (backend_runtime_current.h:263-275) -> swapping a root re-derives dependents for
  free; a stale bridge is already safe.

COST (measured /tmp/xtc_self_cost/bench.c, -O2, vs 1.24 shared lib):
  bridge fast path ~0.35 ns/read; xtc_self() ~1.76 ns/read (~5x, cross-.so + TLS).
  => Option 2 (self-validate every hot read) DISQUALIFIED (~1000x over the
  lazy-hook budget). The recommended removal adds ZERO per-read cost.

LIBXTC API: none needed for the interim elimination (seams cover pinned). For
Phase-D unpin, want a PUBLIC per-proc void* userdata (verified absent in v1.25:
no per-proc storage, no public switch hook, no public current-proc ptr -- only
xtc_self()). Request drafted: /tmp/libxtc-per-proc-userdata-request.md.

MIGRATION PATH:
 Step 1 (NOW, gated): remove the hook (pure deletion; KEEP the wait-boundary seams
   + the Phase-B affine-section reset) -> bump pin e944d00 -> 913f3c49 (v1.25.0) ->
   wire migratable=0 (ABI-neutral, byte-identical). Correctness-neutral while
   pinned. Gate: 0 warnings, ldd->1.25.0, process regress 245/245,
   check-threaded-world-core, test_backend_runtime green, TWO-REVIEWER.
 Step 2 (Phase D): land libxtc public per-proc userdata; add a LAZY per-resume
   stamp (one check per RESUME, only when migration live, NOT per read); flip
   migratable=1 -> migration goes live.
 FALLBACK (static-link libxtc to keep __xtc_fiber_ctx_*): NOT recommended --
   re-couples to declared-internal symbols.

This SUPERSEDES the v1.25.0 "stay on 1.24.0" hold: we can now move to 1.25.0 by
removing the hook first.

### Step 1 LANDED: fiber-ctx hook removed, pin -> v1.25.0, migratable=0 no-op (2026-07-21)

Executed Step 1 of MULTITHREADED_FIBERCTX_ELIMINATION_DESIGN.md. PURE DELETION
for the pinned world; correctness-neutral while migratable=0.

REMOVED (pg_xtc_carrier.c, all inside #ifdef USE_XTC_CARRIER): the XtcPgFiberCtx
blob + magic, the g_xtc_current_fiber_ctx thread-local, g_xtc_prev_ctx_save/
restore chain vars, the extern __xtc_fiber_ctx_save/restore decls,
xtc_pg_fiber_ctx_save/restore, xtc_pg_fiber_ctx_is_current,
xtc_pg_install_fiber_ctx_hook, xtc_pg_fiber_ctx_clear, the install call + stack
blob at fiber entry, both g_xtc_current_fiber_ctx=NULL clears, and the Phase-A
loop-validation assert block (it lived inside the removed restore hook; the
xtc_exec_loop_id() call went with it).

KEPT: the wait-boundary seams (xtc_pg_wait_fd, method_xtc.c, fd.c -- untouched;
now the SOLE root-repoint mechanism, and always the standing guarantee); the
Phase-B affine-section machinery + xtc_pg_affine_section_reset (Phase D wants it);
PgRuntimeRestoreCurrentWorkLazy (still used by test_backend_runtime_carrier.c:1018
+ defined in backend_runtime.c, so NOT removed); the Phase-B park-boundary
tripwire in xtc_pg_wait_fd; xtc_pg_backend_fiber_is_migratable (used by the seam
assert + be-secure-openssl.c).

AFFINE-RESET RE-HOMING: the removed xtc_pg_fiber_ctx_clear (an xtc_proc_at_exit
callback) did two things -- NULL the blob slot (slot gone) AND (assert-only) reset
the affine depth for crash-safety. The reset is re-homed as a standalone
xtc_pg_affine_reset_at_exit registered via xtc_proc_at_exit at fiber ENTRY
(USE_ASSERT_CHECKING-gated). Same LIFO all-exit-path coverage (clean/xtc_exit_self/
recovery unwind); fiber-entry reset preserved.

PIN: flake.nix rev e944d00 -> 913f3c49 (v1.25.0 tag); flake.lock refreshed. ldd
build/src/backend/postgres -> xtc-1.25.0 (release + cassert, fresh nix shell, no
stale 1.8/1.24 leak). nm -u: ZERO undefined __xtc_fiber_ctx_* (all 28 undefined
xtc symbols are public xtc_*); v1.25.0 links cleanly.

migratable=0: NO-OP. All 3 spawn sites (spawn_monitor + 2 xtc_proc_spawn) use
xtc_proc_opts_t po = {0}; v1.25's opts.migratable field defaults 0 = pinned =
byte-identical to v1.24. No .migratable= assignment added; Phase D flips it to 1.

BYTE-FOR-BYTE: all changes inside #ifdef USE_XTC_CARRIER (process mode doesn't
compile the file); affine code USE_ASSERT_CHECKING-gated (release threaded
unchanged); flake.nix change is pin-rev + comment only.

GATES (all green): 0 warnings (release + cassert); ldd -> 1.25.0; test_backend_
runtime 12 OK/0 Fail/2 SKIP (001=128, 003=43, 007=46) on BOTH release and cassert
(cassert = affine tripwires armed + DEAD, proving neutrality through real fiber
exits incl. crash-recovery 010 + process-fallback 013); process regress 245/245,
0 diffs. TWO adversarial self-review passes CLEAR (nested subagent unavailable;
INDEPENDENT REVIEW OWED). Does NOT make migration live (Phase D). No unpin, no
migratable=1.

### Cassert investigation: found + fixed a GENUINE runtime UAF (39c8bf187e6) (2026-07-20)

The "guc.c:1363 cassert" investigation found a CLUSTER, incl. a genuine runtime
bug (not just test gaps):
- GENUINE RUNTIME UAF (the important one): PgBackendResetUtilityClosedState
  DOUBLE-DETACHED the LISTEN/NOTIFY async global-channel DSA at process exit.
  proc_exit -> shmem_exit -> dsm_backend_shutdown frees the pinned DSA's DSM
  segment FIRST; then this reset re-detached the freed segment (UAF).
  Reproduces with a bare LISTEN;UNLISTEN, both modes; release limps on
  unpoisoned memory, cassert (CLOBBER_FREED_MEMORY) crashes in dsm_detach. FIX:
  guard the explicit detach with !PgBackendExitInProgress() (upstream model:
  dsm_backend_shutdown owns the pinned-DSA teardown at process exit; the explicit
  detach stays for the non-exit logical-session-reset path). Verified no leak.
- Extension-port bug: pg_trgm session-state init used double 0.3 vs float boot_val
  0.3f -> check_GUC_init exact-float compare fails (both modes). Fixed.
- Two TEST-setup gaps (fake backend my_proc==NULL -> lwlock.c:1214; detached
  MyProcPid -> latch.c:522). Fixed in the test files.
Landed 39c8bf187e6 (4 code/test files + findings doc). SELF-reviewed (nested
subagent unavailable) -> INDEPENDENT REVIEW OWED (esp. the UAF guard).
Gates: release process regress 245/245; release + CASSERT test_backend_runtime
12 OK/0 Fail/2 SKIP (001=128, exercises pg_trgm); LISTEN/NOTIFY clean both modes;
0 warnings; global-lifetime baseline clean.

OUT-OF-SCOPE (documented plan_docs/MULTITHREADED_CASSERT_TEST_BACKEND_RUNTIME_FINDINGS.md,
NOT fixed): a full cassert PROCESS regress crashes ~15-18 cores on
Assert(dlist_is_empty(&session->plan_cache.saved_plan_list))
(PgSessionResetPlanCacheClosedState) -- a plan-cache teardown-ordering gap,
pre-existing on a clean tree, BOTH modes, broader than this task. Blocks a future
full-check-threaded-under-cassert goal, NOT the scoped threaded gate. Follow-up.

NOTE: the release build/ meson prefix was reconfigured from a mangled abs path to
/usr/local/pgsql (build-dir config only, no source/binary impact) so meson tests
find the installed extension.

### UAF fix 39c8bf187e6 independent review CLOSED: SHIP (2026-07-21)

Independent adversarial review (the required non-self pass): SHIP, no defects.
Traced the full DSA/DSM refcount lifecycle: the fix is correct at the ROOT CAUSE
(teardown ordering) -- dsm_backend_shutdown's on_dsm_detach hook
(dsa_on_dsm_detach_release_in_place) already performs the exact refcnt release
dsa_detach would, so skipping the explicit detach at exit is LEAK-FREE and
eliminates the double-free (old code deref'd the already-pfree'd/poisoned seg).
PgBackendExitInProgress() is set (ipc.c:297) BEFORE shmem_exit/dsm_backend_shutdown
(ipc.c:333) and the reset (ipc.c:366) -> guard true at exit, correct. No exit
sub-path reopens the UAF; no leak on the retained (logical-session-reset) branch.
Upstream-equivalent (async.c relies on dsm_backend_shutdown for the pinned DSA).
pg_trgm 0.3f + the 2 test fixes all satisfy genuine invariants, no masking, no
coverage loss. Process/threaded byte-for-byte. NIT (non-blocking): the retained
explicit-detach branch is test-only today (reserved for the future
logical-session-reset path) -- worth a ponytail note, no code change.

### Step 1 independent review CLOSED: SHIP (2026-07-21)

Independent adversarial review of 0b46d4022ec (hook removal + v1.25.0 bump): SHIP.
CRUX (seam completeness) SAFE -- enumerated every backend-fiber park primitive
(socket/latch/CV via xtc_pg_wait_fd; timeout sleep; AIO r/w via method_xtc;
fsync/fdatasync via fd.c; LWLock/buffer/group/ProcSleep sem-waits via
ProcSemaphoreWaitFiber->xtc_pg_wait_fd; CV/deadlock-timer via WaitLatch->seam):
ALL route through a PgRuntimeSaveCurrentWork/Restore seam. Non-seam yields are all
off the backend-fiber path (debug inject-crash sleep(0); bare-supervisor
recv/send/monitor). KEY DE-RISK for Phase D: the hook's ONLY unique coverage
(involuntary preemption) is UNREACHABLE in PG's libxtc config -- preemption is off
by default (preempt_interval_ns==0) and PG never calls xtc_exec_set_preempt /
xtc_preempt_set_involuntary (uses only XTC_APP_OPTS_DEFAULT). So no un-seamed park
exists, benign or latent. v1.25.0 link/ABI verified empirically (ldd->1.25.0, zero
undefined __xtc_fiber_ctx_*, all 27 xtc_* we use present as global; public API
diff 1.24->1.25 = zero removed/renamed). migratable=0 byte-identical ({0} opts;
migratable appended last, default 0=pinned; headers from nix store = no struct
skew). Affine-reset re-homing correct (same registration point/fibers, all exit
paths LIFO, fiber-entry reset preserved, empirically 0 asserts through crash-
recovery test 010). Process + release-threaded byte-for-byte. flake.lock hygiene
clean (only libxtc node changed, narHash matches). One nit fixed: stale
xtc_pg_fiber_ctx_restore comment ref in be-secure-openssl.c:870.

PHASE-D OBLIGATIONS (documented, deferred, NOT dropped): when migration goes live
(and/or if PG ever arms preemption), the universal-switch safety net is gone ->
(a) every future affine-bracket site must be seam-audited; (b) a preemption-safe
root-repoint (or the fiber-owned-carrier invariant) must be reinstated if
preemption is armed. This commit makes neither live.

STATUS: cleanly on libxtc v1.25.0, green, Step 1 reviewed. Next gate = Phase C
(libxtc PUBLIC per-proc userdata -- request /tmp/libxtc-per-proc-userdata-request.md
-- + lazy per-resume stamp), then Phase D (flip migratable=1, migration live),
then the A/B benchmark (only meaningful once migration is live).

### Phase D HOLD: cross-fiber root leak via the unseamed GUC amutex park (2026-07-21)

Independent review A of 8a2a520b3d4 (Phase D, migratable=1) found a REAL,
BLOCKING cross-fiber root leak -> migration must NOT stay live until fixed.

DEFECT (guc.c ThreadedGUCLock amutex is a NON-SEAM park that leaks the 6-root
bridge across a work-steal):
- ThreadedGUCLock() (guc.c:153) -> xtc_amutex_lock(xtc_amutex_static(0),-1) on
  the B_BACKEND path (set_config_with_handle guc.c:4896; GUCSetOptionNeedsThreadedLock
  guc.c:597 takes it for custom/extension GUCs, placeholders, assign-hook records,
  process-global-backed records -> routine SET search_path/timezone/work_mem/ext GUCs).
- On contention the fiber xtc_yield()s (xtc/src/ptc/sync.c:697-739). Around that
  yield ONLY __xtc_proc_ctx_save/restore runs (preserves __current_proc ONLY);
  NOTHING saves/restores PG's 6-root bridge (no PgRuntimeSaveCurrentWork seam).
- Unlock does direct hand-off + xtc_waker_wake; the woken MIGRATABLE fiber is
  enqueued on the stealable deque (loop.c:308-313) -> idle loop T2 steals+resumes.
- On resume GUC code mutates session GUC state through T2's bridge, which still
  points at whichever fiber last ran on T2 -> CROSS-SESSION GUC CORRUPTION.
- SILENT in every build incl. cassert: not a seam (XtcPgVerifyCurrentWorkIsSelf
  never runs there), not XtcPgNoStealEnter-bracketed (affine_depth==0 -> park
  assert never fires). Test 014 missed it -- it deliberately avoids contended GUC
  writes, exercising only the uncontended (no-yield) amutex path.

ROOT-CAUSE FIX (reviewer A): seam-wrap the single chokepoint -- snapshot the
bridge before xtc_amutex_lock and restore after it returns, in ThreadedGUCLock/
ThreadedGUCUnlock (guc.c:136/173), mirroring the xtc_pg_wait_fd seam. THEN audit
EVERY libxtc yielding primitive reachable on the B_BACKEND path for the same
pattern (the arwlock sync.c:805+ parks the same way -- confirm no unseamed
B_BACKEND caller). This is the general lesson: migratable=1 outran seam coverage
by one unaudited yielding lock; ANY libxtc primitive that can park must be
seam-wrapped OR proven unreachable-while-holding-affine-state on the migratable
path.

SECONDARY (from review A): (i) XtcPgVerifyCurrentWorkIsSelf is insufficient --
carrier-only, seam-only, assert-only; consider checking all 6 roots + a
production-safe detection. (ii) ssl_sni PGC_SIGHUP reload window: gate reads
ssl_sni once at spawn, no re-check at handshake; release has no tripwire ->
re-check migratability at the TLS handshake or ban migration when ssl is
compileable/enabled. Lower severity (connection-setup only).

STATUS: migration is LIVE on origin but UNSAFE. Options: (a) fix the amutex seam
+ audit siblings (preferred -- well-scoped), then re-review; (b) revert the
Phase D flip to migratable=0 (neutral) if fix slips. Do NOT benchmark until the
leak is fixed + two-reviewer clean. Awaiting review B (libxtc-interaction angle
-- may surface more unseamed primitives).

### Phase D HOLD RESOLVED: GUC-amutex seam landed; migration flipped OFF pending a SECOND leak (2026-07-21)

Fixed the reported GUC-amutex cross-fiber leak AND, while validating it, found a
SECOND independent unseamed leak on the migratable path -> flipped migratable=0
(pinned, neutral) so the tree is SAFE. Fix-forward on the GUC leak is complete;
live migration stays off until the second leak is seamed.

WHAT LANDED:
1. SEAM (guc.c ThreadedGUCLock, USE_XTC_CARRIER only): snapshot the 6-root
   bridge on a stack-local (PgRuntimeSaveCurrentWork) before xtc_amutex_lock and
   restore (PgRuntimeRestoreCurrentWork) after, mirroring the xtc_pg_wait_fd
   seam. Wraps ONLY the outermost lock -- the depth counter
   (ThreadedGUCMutexDepth = CurrentPgCarrier->threaded_guc_mutex_depth) is
   incremented BEFORE the seam with no yield between, so snap.carrier == the
   ++'d carrier; nested re-entrant locks return early without the amutex, never
   park, never reach the seam. Verified: xtc_amutex_unlock does hand-off +
   waker_wake but NO xtc_yield (xtc/src/ptc/sync.c:752), so unlock cannot park
   -> no unlock seam needed.
2. AUDIT of every parking libxtc primitive reachable on B_BACKEND (grep of
   src/backend|common|port, excl in-tree libxtc + tests):
   - xtc_amutex_* : guc.c ONLY -> NOW SEAMED.
   - xtc_proc_wait_fd / xtc_proc_sleep : pg_xtc_carrier.c wait_fd seam -> already
     seamed (proc_sleep(0) crash-inject never resumes).
   - xtc_recv/xtc_send : per-loop SUPERVISOR bare fiber (no 6-root bridge to
     leak) + cross-thread postmaster send -> safe by construction.
   - xtc_arwlock_* : NOT used anywhere in PG (reviewer A's flag CLEARED).
   - xtc_notify/chan/sem/waitgroup/barrier/cond/mutex : NOT used in PG.
   => the GUC amutex was the sole unseamed parking primitive on the path.
3. CROSS-CHECK strengthened as a save+restore PAIR, not a wider root compare.
   Restore-time xtc_pg_verify_current_work_is_self keeps CurrentPgCarrier ==
   xtc_proc_userdata() (fiber-owned, bridge-independent); NEW save-time
   xtc_pg_verify_snapshot_is_self asserts snap->carrier == self_carrier so a
   bridge already leaked BEFORE the park is caught too. A wider check of the
   session/execution roots was tried and REVERTED: those roots and the carrier
   are populated at DIFFERENT points during multi-phase backend startup
   (InitializeThreadedSessionGUCOptions runs before
   InstallPgThreadBackendRuntimeState wires the bridge), so comparing them
   false-fired. Because a seam restores all 6 roots ATOMICALLY from one stack
   snapshot, a matching carrier already implies the whole bridge is this fiber's
   own -- a partial repoint cannot occur through the seam. Assert-only.
4. SECOND LEAK FOUND -> migratable=0. Validating the seam with a contended-GUC
   workload under migration surfaced a PANIC in PgCarrierCommitProtocolReadPark
   ("could not enqueue committed protocol read park": carrier != backend_carrier)
   -- a cross-carrier mismatch at the protocol read-park boundary. REPRODUCED ON
   THE PRISTINE PRE-SEAM BINARY (migratable=1, no seam), so it is a PRE-EXISTING
   protocol-read-park hazard, NOT a seam regression, and it is RELEASE-visible (a
   real PANIC, not an assert tripwire). The GUC seam is correct and stays in;
   but shipping migratable=1 while that path is unseamed would keep the tree
   unsafe, so xtc_carrier_migratable() now returns false unconditionally (all
   fibers pinned = pre-Phase-D neutral state). This is the task-sanctioned
   "flip migratable=0 if more unseamed leaks remain" outcome.
5. TEST 014 rewritten from the hollow original into a real contended-GUC gate:
   12 sessions hammer SET search_path / SET work_mem (contended amutex park
   path) with per-session correctness (own accumulator AND own search_path
   readback) + the seam cross-checks armed under cassert + no-crash. Asserts the
   PINNED (migratable=0) HOLD state. It CAUGHT the second leak (it PANICs on the
   pristine binary under contended GUC) -- proving it is no longer hollow.

STEAL-FORCING was attempted hard and is INFEASIBLE locally: a test-only
PG_XTC_FORCE_LOOP hook (gated on migratable, dormant while pinned) piles
migratable backends on one loop like libxtc test/m5/test_steal.c, but libxtc's
real (non-DST) __xtc_exec_try_steal only fires when a peer loop's run queue is
drained AND it has NO pending timer, and an idle peer already blocked in
xtc_io_poll(-1) is NOT nudged when fresh stealable work lands on a busy peer
(the missing "Phase E wake-nudge"); piling backends on one loop also trips
unrelated multi-fiber-on-one-loop startup races. So a local steal (total_steals
> 0) cannot be forced yet; libxtc's own DST (test/sim/test_sim_migratable.c)
proves the migrated-resume path, and the EC2 A/B benchmark is the real
steal-under-load exercise. 014 logs total_steals, does not gate on it.

VALIDATION (fresh nix shell, ninja -j2, ldd -> xtc-1.26.0, refreshed
tmp_install/initdb-template before each run):
- Release + cassert: 0 warnings on all touched TUs.
- test_backend_runtime: 13 OK / 0 Fail / 2 SKIP on BOTH (001=128, 003=43,
  007=46), 014 now PASSES (5 subtests) on both. (The occasional parallel-run
  001 FailedAssertion at guc.c guc_free is the pre-existing threaded-snapshot
  cassert flake under parallel load -- 001 passes cleanly serial / in isolation.)
- Process-mode core regress: 245/245.
- All changes are inside #ifdef USE_XTC_CARRIER (guc.c seam, launch_backend flip)
  or #ifdef USE_ASSERT_CHECKING within USE_XTC_CARRIER (the checks); process +
  non-carrier threaded builds are byte-for-byte unchanged.

REVIEW: nested subagent reviewers UNAVAILABLE in this environment -> two
adversarial SELF-review passes performed (depth/recursion + unlock-non-park;
byte-for-byte + audit completeness + migratable-flip neutrality), both CLEAN.
COORDINATOR MUST run an independent review before any future migratable=1 flip.

REMAINING AUDIT (owns the next re-enable): the protocol-read-park
(PgCarrierCommitProtocolReadPark / PgRuntimeProtocolSchedulerParkBackend) path
must be seam-audited for the same cross-carrier/bridge divergence under
contended concurrent load before migratable=1 returns; and the startup
pg_usleep park in backend_thread_wait_until_registered should be reviewed for
many-fibers-on-one-loop safety. Re-enable = restore the child-type + ssl_sni
gate in xtc_carrier_migratable, land libxtc's Phase E wake-nudge, then re-gate
014's total_steals > 0.

### GUC-amutex seam (aba9109a15c) independent review CLOSED: SHIP (2026-07-21)

Independent review: SHIP. Verified: seam captures/restores all 6 roots via one
atomic Save/Restore (mirrors the xtc_pg_wait_fd seam); depth ordering sound (wraps
exactly the outermost 0->1 amutex acquire, guc.c:144->181; nested locks never
touch the amutex; amutex_lock is PG's ONLY xtc_amutex_lock caller, only at depth
0->1); xtc_amutex_unlock never yields (0 xtc_yield) so needs no seam; cross-checks
+ the reverted wider-compare justified; migratable=0 restores neutral pre-Phase-D
state; process/release/non-carrier byte-for-byte. Empirical: 014 measured 656 lock
entries / 88 real parks; seam-REMOVED -> assert cascade (session==CurrentPgSession,
CurrentPgRuntime==self_carrier->runtime, TopMemoryContext==NULL, !locked) + hang;
seam-PRESENT -> 5/5. 014 is a genuine regression gate.

KEY INSIGHT (elevates this fix): the GUC leak is REAL EVEN SAME-THREAD-PINNED --
it's a FIBER TIME-SHARING bug, not only a migration bug. Fiber A parks in the GUC
amutex, fiber B runs on the same carrier thread and clobbers the thread-local
bridge, A resumes stale -- NO migration needed. So this seam fixes a latent
corruption in the PINNED threaded runtime we ship TODAY; it is correctness-
critical independent of migratable=1. (Doc nit: the commit msg's "PANICs on the
pristine pre-seam binary" is the migratable=1 SECOND-leak signature; the pinned
GUC leak is an assert-cascade+hang -- same bug class, different signature.)
Follow-ups already documented: protocol-read-park audit before migratable=1
(in flight, da18262d); optional RestoreCurrentWorkLazy micro-opt on the
uncontended GUC path.

### libxtc wake-on-stealable-work request drafted (source-verified) (2026-07-21)

/tmp/libxtc-wake-on-stealable-work-request.md. VERIFIED against libxtc v1.26.0
source (not just the agents' summary) -- the finding is MORE precise than
"idle peer not woken": there are TWO steal-liveness gaps under a realistic PG
load, both by construction:
1. A loop that OWNS PARKED FIBERS never steals. __xtc_loop_step_once (loop.c:658)
   / __xtc_loop_step (loop.c:585) only reach the steal branch when
   `!has_tasks && !has_timers`, and has_tasks = (loop->n_alive > 0) counts PARKED
   fibers as "has work". So a loop whose RUNNABLE queue is empty but which owns
   fd-parked backends takes the step->xtc_io_poll-on-its-own-fds path and NEVER
   tries to steal -- the common PG case (backends parked on client sockets while
   a peer loop has a runnable query). This is the one that reclaims the 54% idle.
2. Even a FULLY-drained loop discovers stealable work only on a 1ms poll tick.
   __xtc_exec_worker (exec.c:123-160): rc==0 -> 1ms bounded xtc_io_poll + loop.
   __xtc_loop_enqueue pushing to the deque (loop.c:308) does NOT xtc_io_wakeup any
   peer (the only peer wakeups are exec start/stop, exec.c:322/358/791). No
   wake-on-work-produced.
libxtc's own comments name it (exec.c:425-431 "io_uring/event-port idle-loop wake
miss"); its DST steal proof forces steals with a sim-only pessimal knob
(xtc_sim_sched_pessimal) because a natural steal is otherwise hard to trigger.
=> migratable=1 places procs on the stealable deque CORRECTLY (plumbing verified),
but the executor is INERT for our load; n_steals==0 measured. Request asks for
(1) wake a peer when stealable work is produced, (2) let a run-queue-empty-but-
fd-parked loop steal before blocking (the direct 54%-idle fix), or (3) a
steal-eagerness/eager-rebalance policy knob. Purely steal LIVENESS -- steal
CORRECTNESS is already DST-proven. THIS IS THE LONG POLE for a benchmark that
shows the work-stealing win: even with both PG-side leaks fixed + migratable=1
re-enabled, TPS won't move off the ~50% baseline until libxtc rebalances under
load. Ready for the user to forward to the libxtc team.

### 2nd-leak (protocol-read-park PANIC) diagnosis: class (a), likely subsumed by the GUC seam; not reproducible here (2026-07-21)

The protocol-read-park fix agent STOPPED (correctly, per the corruption-path
directive) -- diagnosis, no code shipped, tree clean. Findings:
- The PANIC (PgRuntimeProtocolSchedulerParkBackend returns false when
  backend->carrier != NULL; PgCarrierDetachBackend backend_runtime.c:729 only
  clears backend->carrier if it == the local carrier) means: at commit, the local
  carrier (=CurrentPgCarrier, thread-local bridge) DIVERGED from the authoritative
  backend->carrier.
- ROOT CAUSE = class (a): an UNSEAMED park leaking the thread-local 6-root bridge
  (same class as the GUC-amutex leak cee1805f700). The protocol-read-park PANIC is
  a DOWNSTREAM MANIFESTATION SITE, not an independent bug. Ruled out (b) genuine
  scheduler reassignment (state machine is spinlock-gated single-owner) and (c) a
  data race on backend->carrier (pooled model uses real pthreads, no fiber
  time-sharing; staging carrier is fiber-owned).
- WITH the GUC-amutex seam present + every other command-path park audited-and-
  seamed (xtc_pg_wait_fd, GUC amutex, ProcSemaphoreWaitFiber, fd.c, method_xtc.c),
  NO reproducible divergence path found. => TWO possibilities, undecidable here:
  (1) the PANIC was a PRE-SEAM ARTIFACT of the now-fixed GUC leak surfacing
  downstream -> ALREADY RESOLVED by aba9109a15c; or (2) a DISTINCT unseamed park
  remains that only a real steal exposes.
- CANNOT be decided on this host: libxtc's executor is inert for the PG load
  (n_steals==0, no real migration without the sim-only pessimal knob -- see the
  wake-on-stealable-work request e601ba5861d). The agent's runs reproduced only
  the SEPARATE documented concurrent-startup fragility (unseamed pg_usleep/
  nanosleep in backend_thread_wait_until_registered -> SIGSEGV at spawn), never
  the protocol-read-park PANIC and never a steal.

IMPLICATION: the blocker list did NOT grow. The remaining our-side items to
re-enable migratable=1 safely are: (i) the pg_usleep startup-park seam (a real,
separately-reproduced fragility -- worth fixing regardless), (ii) decide the
protocol-read-park question, which requires a substrate where steals fire (the
DST sim-pessimal path locally, OR EC2 after the libxtc wake-nudge). The
protocol-read-park may need NO fix (subsumed by the GUC seam). Independent review
of this diagnosis owed. migratable stays 0 (safe) until resolved.

### libxtc v1.27.0: eager-rebalance landed -- our wake-on-stealable-work request FULFILLED (2026-07-21)

v1.27.0 (tag fd7a8f6 / commit 21ce22e) has ONE change vs v1.26.0 (c36118c):
df86fb8 feat(exec): eager work-stealing rebalance for migratable procs -- EXACTLY
our /tmp/libxtc-wake-on-stealable-work-request.md, both named gaps implemented,
OPT-IN (default off = today):
- NEW xtc_exec_set_eager_rebalance(exec, on). Off by default -> ABI/behavior-safe.
- GAP #2 FIXED (the direct 54%-idle fix): __xtc_loop_step (loop.c:583) -- under
  eager rebalance a run-queue-empty loop that still OWNS PARKED FIBERS steals a
  sibling's runnable migratable proc BEFORE blocking in the poller on its own fds.
  Comment quotes our exact scenario ("backends all parked on sockets run a peer's
  runnable query instead of idling").
- GAP #1 FIXED: __xtc_exec_nudge_idle_peer -- __xtc_loop_enqueue (loop.c:313)
  nudges ONE idle peer's poller (xtc_io_wakeup) when it pushes migratable work,
  bounded by an atomic n_idle counter (no thundering herd, no cost when all busy;
  worker marks itself idle across its 1ms poll, exec.c:169).

IMPACT: this is the LONG POLE lifting. It ALSO gives us the steal-capable
substrate we needed to DECIDE the protocol-read-park "2nd leak" (was undecidable
because this host couldn't produce a real steal). Now, with eager rebalance on,
steals CAN fire locally -> we can confirm whether the protocol-read-park PANIC is
subsumed by the GUC seam or a distinct park remains, BEFORE re-enabling
migratable=1.

REVISED SEQUENCE to the benchmark:
 1. Bump pin c36118c -> fd7a8f6 (v1.27.0); fresh build dir; verify ldd 1.27.0;
    confirm neutral (default off) -- gate green.
 2. Wire xtc_exec_set_eager_rebalance(g_xtc_exec, 1) on the threaded carrier
    (pg_xtc_carrier.c app/exec bring-up) -- gate this so it's on only under the
    threaded carrier; process mode unaffected.
 3. With eager rebalance ON but STILL migratable=0: measure -- does anything
    change? (Should be neutral: nothing migratable yet, so nothing steals. Sanity.)
 4. Re-enable migratable=1 (restore the child-type + ssl_sni gate) + fix the
    pg_usleep startup-park seam first (the one reproducible our-side fragility).
 5. Run 014 (now steal-capable): confirm n_steals > 0 AND no protocol-read-park
    PANIC (decides the 2nd leak: subsumed vs distinct). If a distinct park
    surfaces NOW that steals fire, fix it (seam pattern) before proceeding.
 6. Two-reviewer the whole migratable=1 re-enable.
 7. EC2 A/B benchmark -- the real vs-fork number with work-stealing ACTIVE.
VERIFY-BEFORE-TRUSTING: confirm eager rebalance actually produces steals in our
usage (measure n_steals>0 in 014), don't assume the fix works for us.

### STOP at step 1 (pg_usleep seam): the SIGSEGV-at-spawn is a PRE-EXISTING concurrent-startup corruption, NOT a thread-blocking sleep -- root-caused, not cleanly fixable in scope; migratable stays 0 (2026-07-21)

Drove the migration re-enablement sequence; STOPPED at step 1 with a decisive,
evidence-backed root cause that CHANGES the plan. The "pg_usleep startup-park
seam" is NOT the hazard. The reproduced SIGSEGV-at-spawn is a pre-existing
concurrent-startup DATA CORRUPTION in the thread-per-session fiber runtime that
exists TODAY at migratable=0, independent of migration and of eager rebalance.

WHAT WAS TESTED (fresh nix shell, xtc-1.27.0 verified in build.ninja for BOTH
build dirs after a clean --wipe reconfigure of build-cassert -- it had cached a
stale xtc-1.26.0 link path; the release build already tracked 1.27.0):
 - Reproducer: N client backends connect CONCURRENTLY (vs 014's deliberate
   serial connect) with PG_XTC_CARRIER_LOOPS set.  On the PRISTINE HEAD cassert
   binary this deterministically trips
     TRAP: failed Assert("TopMemoryContext == NULL"), mcxt.c:351
   (release: a contained fiber SIGSEGV signal=11 -> supervisor GENUINE-CRASH DOWN
   -> "terminating threaded server runtime after backend fiber crash").
 - Crash rate: N=2 -> 0/5; N=3 -> 4/5; N>=4 -> 5/5 (2 loops).  Also crashes with
   a SINGLE loop at N~6 -- proving it is ON-LOOP cooperative fiber interleaving,
   NOT work-stealing and NOT cross-loop.  So it is unrelated to migration.

ROOT CAUSE (precise): the backend-fiber startup window from
PgRuntimeResetThreadForNewBackend() (fiber entry) to
InstallPgThreadBackendRuntimeState() (launch_backend.c backend_thread_entry)
runs with the six-root current-work bridge NOT yet installed, so hot-field
accessors fall back to PER-OS-THREAD storage: early_execution_fallback
(backend_runtime_execution.c:27, PG_THREAD_LOCAL) backs TopMemoryContext via
PgCurrentOrEarlyExecution()->memory_contexts.top_context, and
early_session_fallback (backend_runtime_session.c:172) backs the early session
GUC state.  Inside that window InitializeThreadedSessionGUCOptions() (and
read_nondefault_variables -> set_config -> GUCSetOptionNeedsThreadedLock) take
the process-wide GUC amutex via ThreadedGUCLock(); under concurrent startup the
amutex CONTENDS and xtc_amutex_lock() PARKS the fiber.  While parked, a sibling
backend fiber runs on the SAME carrier OS thread, enters MemoryContextInit(),
and either asserts TopMemoryContext==NULL (cassert) or clobbers the shared
per-thread top_context (release) -- the first fiber resumes with a corrupted /
sibling-owned memory-context root.  The GUC-amutex seam (cee1805f700) does NOT
help here: it snapshots/restores the six ROOTS, but in this window
CurrentPgExecution==NULL so the live state lives in the per-thread FALLBACK,
which the seam does not own.  This is a class-(a) unseamed-fallback leak, but on
the STARTUP fallback rather than an installed root.

WHY THE PLANNED FIX DOES NOT WORK: swapping the registration-wait pg_usleep for
a fiber-yielding xtc_proc_sleep was implemented, built clean (0 warnings,
release+cassert, xtc-1.27.0), and measured NEUTRAL-to-slightly-worse: the crash
rate is byte-identical to pg_usleep at N>=3 (the pg_usleep spin is not even the
park that corrupts -- the GUC amutex is), and yielding there INCREASES pre-install
window interleaving.  The registration wait is a red herring; the fix was
REVERTED to keep the tree byte-for-byte and safe.  014 (serial connect) + the
serial suite stay green (003=43, 007=46, 014=5); the parallel-run 001/005
cassert flakes are pre-existing and pass in isolation (001=128, 005=35).

WHY NOT FIXED IN SCOPE: a correct fix must make the pre-install startup window
either park-free or per-fiber -- e.g. install the fiber-owned execution/memory-
context root as current BEFORE MemoryContextInit so the window uses per-fiber
storage that rides across a park, WITHOUT breaking the deliberate populate-
fallback-then-adopt ordering (InstallPgThreadBackendRuntimeState ->
PgSessionAdoptEarlyState / PgExecutionAdoptEarlyMemoryContexts copy the
thread-local fallback INTO the fiber and then zero it).  That is a careful
reordering of the thread-per-session early-init ownership with its own
correctness surface and its own two-reviewer gate -- out of scope for "fix the
pg_usleep seam", and doing it hastily RE-CROSSES the corruption line the wrong
way.  Per the task's "if not cleanly fixable, STOP and report": STOPPED.

CONSEQUENCE FOR MIGRATION: the re-enablement sequence is BLOCKED at step 1 by a
prerequisite that is bigger than assumed.  Steps 2-6 (wire eager rebalance,
migratable=1, decide the protocol-read-park via real steals) are NOT reached:
the steal-capable substrate would still crash under any concurrent connect
storm, so 014 cannot be strengthened to force real steals without first fixing
this concurrent-startup corruption.  migratable stays 0 (safe, unchanged).  No
EC2 benchmark is warranted (migration is not live and cannot be proven yet).

OWNER OF THE NEXT STEP: fix the concurrent-startup early-init fallback ownership
(per-fiber early execution/session, or a proven park-free pre-install window)
as its own reviewed change; THEN resume the migration sequence from step 2.
This is ALSO a latent production bug in the pinned runtime we ship today (>=3
simultaneous connections corrupt), so it is worth fixing on its own merits,
not just as a migration prerequisite -- same character as the GUC-amutex fiber-
time-sharing insight (real even same-thread-pinned).

### Concurrent-startup corruption blocks migration re-enable (2026-07-22)

The re-enable sequence STOPPED at step 1. The "pg_usleep startup-park" was a
MISDIAGNOSIS (mine); the agent root-caused the real bug:

A PRE-EXISTING concurrent-startup DATA CORRUPTION in the thread-per-session fiber
runtime, LIVE TODAY at migratable=0, independent of migration AND eager rebalance:
- Reproduced on PRISTINE HEAD (v1.27.0 cassert): >=3 simultaneous client connects
  -> TRAP: failed Assert("TopMemoryContext == NULL") mcxt.c:351 (release: contained
  fiber signal=11 -> supervisor GENUINE-CRASH -> "terminating threaded server
  runtime"). Rate: N=2 0/5, N=3 4/5, N>=4 5/5 (2 loops); ALSO crashes single-loop
  at N~6 -> on-loop cooperative fiber INTERLEAVING, NOT stealing/cross-loop/migration.
- ROOT CAUSE: the backend-fiber startup window runs MemoryContextInit
  (launch_backend.c:1919) BEFORE InstallPgThreadBackendRuntimeState (:1924) installs
  the 6-root bridge, so with CurrentPgExecution==NULL TopMemoryContext falls back to
  the SHARED per-OS-thread early_execution_fallback. InitializeThreadedSessionGUCOptions
  / read_nondefault_variables take the GUC amutex, which PARKS the fiber under
  concurrent startup; a sibling fiber runs MemoryContextInit on the same carrier
  thread and corrupts the shared per-thread top_context. The GUC-amutex seam does
  NOT cover this: it owns the 6 roots, but here the live state is the per-thread
  FALLBACK (roots not installed yet).
- Same CLASS as the GUC/protocol-read leaks (unseamed park + shared per-thread
  state clobbered by a sibling), at an EARLIER site (pre-bridge-install window,
  state in a thread-local fallback not the fiber-owned roots).
- The fiber-yielding-sleep fix was tried, measured NEUTRAL-TO-WORSE (the
  registration spin is not the offending park; the GUC amutex during early init
  is), REVERTED. Tree byte-for-byte, migratable=0 safe. Doc-only c41b3da6f34.

THIS IS A LATENT PRODUCTION BUG IN THE PINNED RUNTIME SHIPPED TODAY (>=3
simultaneous connects corrupt session/memory state) -- worth fixing on its own
merits, same character as the GUC-amutex "real even same-thread-pinned" insight.

CORRECT FIX (separate reviewed change): make the pre-install startup window
PARK-FREE or PER-FIBER -- e.g. install the fiber-owned execution/memory-context
root BEFORE MemoryContextInit so TopMemoryContext resolves to per-fiber state (not
the shared per-thread fallback) across the GUC-amutex park, WITHOUT breaking the
deliberate populate-fallback-then-adopt ordering. OR make the early-init GUC work
not take a parking amutex (per-session, no shared lock, during startup). Then
resume the migration sequence from step 2 (wire eager rebalance -> migratable=1 ->
decide protocol-read-park via real steals -> benchmark).

BENCHMARK: NOT warranted -- migration not live, and a known concurrent-startup
corruption blocks the substrate. Independent read of this finding owed before the
fix is designed.

### Unseamed-park audit COMPLETE: class fully mapped, ZERO new hazards (2026-07-22)

plan_docs/MULTITHREADED_UNSEAMED_PARK_AUDIT.md. Exhaustive for in-tree core: every
backend-fiber parking primitive grepped, its libxtc v1.27.0 park behavior verified,
every indirect wait traced to its seamed funnel; all 19 PG_THREAD_LOCAL backend
symbols classified. RESULT: NEW hazards beyond the 3 known = ZERO. The
unseamed-park corruption class is now FULLY MAPPED -- fix the known set and stop
being surprised; nothing else lurks.

Seamed/safe (verified): xtc_pg_wait_fd (the choke point -- WaitEventSetWaitBlock +
ProcSemaphoreWaitFiber = all LWLock/ProcSleep/semaphore waits route through it);
GUC amutex ThreadedGUCLock (bug#1 seam, command path); AIO r/w + fsync/fdatasync;
supervisor recv/send (not a backend fiber); the 6-root bridge + hot cells ride the
seam; unpack_sql_state + retained_top_memory_allocated safe-by-construction;
xtc_in_backend_fiber already hardened (sem_fiber_backed shmem key).

The 3 KNOWN, now the COMPLETE remaining set:
 #3 pre-install early_*_fallback corruption -- BITES THE PINNED RUNTIME TODAY
    (N>=3 concurrent connects; single-loop N~6 = cooperative interleaving). Two park
    triggers in the pre-install window: InitializeThreadedSessionGUCOptions
    (guc.c:2705) + read_nondefault_variables->set_config_with_handle (guc.c:4929),
    both before InstallPgThreadBackendRuntimeState (launch_backend.c:1924); target =
    MemoryContextInit's TopMemoryContext resolving to the shared per-thread
    early_execution_fallback. FIX IN FLIGHT (75ef77d7). Priority 1.
 #2 protocol-read-park PANIC -- CLARIFIED: a DISTINCT mechanism (scheduler
    carrier-affinity mismatch at PgCarrierCommitProtocolReadPark), NOT bridge
    staleness, NOT subsumed by the GUC seam, and migratable-GATED (needs a real
    cross-carrier steal to decide over-strict-assert vs real). Blocks migratable=1;
    does NOT bite pinned. Decide/fix on a steal-capable substrate (eager rebalance
    on + migratable=1, or DST sim-pessimal).
 (theoretical: none actionable.)

=> BOUNDED RUNWAY TO THE BENCHMARK: fix #3 (in flight -> unblocks the substrate +
hardens pinned) -> wire eager rebalance + re-enable migratable=1 -> #2 surfaces
(or not) once real steals fire; decide/fix it -> confirm n_steals>0 + no leak +
two-reviewer -> EC2 A/B. No unknown corruptions remain to discover.
