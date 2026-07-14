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
