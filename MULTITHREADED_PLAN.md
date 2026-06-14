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
7. only then make sessions movable across pooled carriers.

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
`sigsetjmp` boundary, while `PgSessionStepUnprotected()` remains private.
`PgSessionRun()` is the process-mode loop that repeatedly invokes that protected
step with a single-message budget.

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
    backend-model metadata;
  - the bundled `pg_stash_advice` persistence worker has an initial
    thread-carrier slice through explicit background-worker backend-model
    metadata, with its `pg_plan_advice` dependency marked for the same
    backend model.
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

Status: in progress. See `MULTITHREADED_PHASE12_STATE.md` for the initial
`CurrentSession` compatibility bridge through `PgSession` and the first
backend-local interrupt holdoff bridge through `PgBackend`, plus the initial
execution debug-state bridge through `PgExecution` and the first connection
socket I/O, protocol dispatch, connection identity, and connection interrupt
flag bridges through `PgConnection`, including the frontend protocol version in
the connection protocol-state bucket, backend startup/authentication state, and
authenticated client connection information, plus the backend pending
interrupt flag bridge and core backend identity/lifecycle state bridge through
`PgBackend`, and the execution error-context/exception stack bridge through
`PgExecution`, plus the core execution memory-context pointer bridge and
transaction resource-owner current-pointer bridge through `PgExecution`, and
the current database identity/path bridge through `PgSession`, plus the parsed
`DateStyle`/`DateOrder` bridge through `PgSession`, and the first
direct-pointer GUC bridges for `IntervalStyle` and query-memory settings
through `PgSession`, plus the planner cost/parallel-planner direct-pointer GUC
bridge through `PgSession`, plus the planner method/tuning direct-pointer GUC
bridge through `PgSession`, plus the tablespace direct-pointer GUC and
binary-upgrade tablespace OID bridge through `PgSession`, plus the remaining
binary-upgrade catalog handoff state bridge through `PgSession`, plus the
parser direct-pointer GUC bridge through `PgSession`, plus the vacuum/analyze
maintenance direct-pointer GUC bridge through `PgSession`, plus the buffer I/O
tuning direct-pointer GUC bridge through `PgSession`, plus the transaction
default direct-pointer GUC bridge through `PgSession`, plus the lock/wait
timeout and lock debug direct-pointer GUC bridge through `PgSession`, plus the
logging/debug direct-pointer GUC and derived `log_min_messages`/backtrace
bridges through `PgSession`, plus the miscellaneous session GUC bridge for
system-table modification, stack-depth, preload-library, and dynamic-library
path state through `PgSession`, plus the pgstat tracking/session-end and
session-report state bridge through `PgSession`, plus the query-ID GUC and
derived enablement flag bridge through `PgSession`, plus the storage
direct-pointer GUC bridge for checksum-failure handling and file-copy method
state through `PgSession`, plus the user/role direct-pointer GUC and derived
`createrole_self_grant` assign-hook state bridge through `PgSession`, plus
the command/trigger/notify direct-pointer GUC bridge for
`session_replication_role`, `event_triggers`, and `trace_notify` through
`PgSession`, plus the replication direct-pointer GUC bridge for walsender,
walreceiver, and logical-decoding state through `PgSession`, plus the general
direct-pointer GUC bridge for security/function flags, temp file/buffer
limits, role bookkeeping, large-object compatibility, float/bytea/XML binary
formatting, identifier quoting, plan-cache mode, and GiN limits through
`PgSession`, plus the access/WAL direct-pointer GUC bridge for table AM,
sequential-scan synchronization, TOAST/WAL compression, WAL initialization and
recycling, WAL consistency checking, commit delay, WAL I/O timing, and
WAL-skipping threshold state through `PgSession`, plus the JIT direct-pointer
GUC bridge for provider selection, enablement, cost thresholds,
expression/deforming toggles, and debugging/profiling/bitcode flags through
`PgSession`, plus the extension-control path and sort direct-pointer GUC
bridge through `PgSession`, plus the text-search and timezone session
environment bridge through `PgSession`, plus the connection/tcop exported GUC
bridge through `PgSession`, plus the server/config-file identity GUC bridge
through `PgRuntime`, plus the prepared-statement storage bridge through
`PgSession`, plus the temporary-table ON COMMIT action bridge through
`PgSession`, plus the sequence `nextval`/`currval`/`lastval` cache bridge
through `PgSession`, plus the parser operator lookup cache bridge through
`PgSession`, plus the regex ctype probe cache bridge through `PgSession`, plus
the large-object relation-handle cache bridge through `PgSession`, plus the
async notification listener-state bridge through `PgSession`, plus the
encoding/conversion cache and selected-encoding state bridge through
`PgSession`, plus the temporary-file accounting and temp-tablespace selection
state bridge through `PgSession`, plus the `array_nulls` and `xmloption`
direct-pointer GUC completion bridge through `PgSession`, plus the SQL
random-function PRNG state bridge through `PgSession`, plus the optimizer
extension-ID and predicate proof-cache state bridge through `PgSession`, plus
the saved-plan and cached-expression plan-cache list bridge through
`PgSession`, plus the namespace/search-path, temporary namespace, and
search-path cache bridge through `PgSession`, plus the locale GUC,
localization, localeconv, and collation-cache bridge through `PgSession`, plus
the authenticated/session/outer/current user identity and security context
bridge through `PgSession`, plus the SSL/GSS connection security-state bridge
through `PgConnection`, plus the PAM authentication scratch-state bridge
through `PgConnection`, plus the backend default PRNG bridge through
`PgBackend`, plus the SPI API/result and private connection-stack bridge
through `PgExecution`, plus the active portal bridge through `PgExecution`,
plus the connection output/check-interval bridge through `PgConnection`,
plus the connection startup timing bridge through `PgConnection`, plus the
extended-query transaction-started loop flag bridge through `PgSession`, plus
the vacuum cost/failsafe and parallel-vacuum execution-state bridge through
`PgExecution`, plus the node read/write scratch-state bridge through
`PgExecution`, plus the basebackup checksum/recovery execution-state bridge
through `PgExecution`, plus the ANALYZE memory-context and buffer-access
strategy execution-state bridge through `PgExecution`, plus the extension
creation execution-state bridge through `PgExecution`, plus the
materialized-view maintenance-depth execution-state bridge through
`PgExecution`, plus the lock-manager backend-local state bridge through
`PgBackendLockState`, plus the transaction/access-manager backend-local state
bridge through `PgBackendTransactionState`, plus the ProcArray
visibility-horizon and XID-cache state bridge through
`PgBackendTransactionState`, plus the backend-status activity snapshot bridge
through `PgBackendActivityState`, plus the pgstat shared-entry reference-cache
bridge through `PgBackendPgStatPendingState`, plus the always-built LWLock
backend-local state bridge through `PgBackendLockState`, plus the dynahash,
superuser-cache, resource-owner callback, and optional resource-owner stats
utility-state bridge through `PgBackendUtilityState`, plus the date/time,
float, formatting, libxml-context, and missing-attribute utility-cache state
bridge through `PgBackendUtilityState`, plus the parallel-worker,
parallel-context, and pqmq backend-local state bridge through
`PgBackendParallelState`, plus the DSM initialization, DSM registry, local
latch, and latch wait-set bridge through `PgBackendIPCState`, plus the timeout
scheduler state bridge through `PgBackendTimeoutState`, plus the
allocation-set freelist and memory-context logging guard bridge through
`PgBackendMemoryManagerState`, plus the wait-event storage bridge through
`PgBackendWaitState` and the shared-invalidation local transaction ID bridge
through `PgBackendIPCState`, plus the command-loop read-boundary bridge
through `PgSessionLoopState` and tcop command-timing/elog line-format state
bridge through `PgBackendCommandState` and `PgBackendLogState`, plus the
backend-local cumulative statistics anchor bridge through
`PgBackendPgStatPendingState`, plus the computed-goto expression interpreter
dispatch/reverse-lookup bridge through `PgBackendExprInterpState`, plus the
optional LWLock debug-statistics hash, dummy entry, memory context, and
exit-registration state bridge through `PgBackendLockState`, plus the
snapshot-manager and combo-CID transaction visibility state bridge through
`PgExecution`, plus the WAL record-construction workspace bridge through
`PgExecution`, plus the simple exported transaction flag/state bridge through
`PgExecution`, plus the GUC/error-report scratch state bridge through
`PgExecution`, plus the miscellaneous array typanalyze, regex locale,
Valgrind, and snapshot-builder scratch-state bridge through `PgExecution`,
plus the attribute-options, relfilenumber, tablespace-options, event-trigger,
ruleutils SPI-plan, and ICU converter catalog lookup/cache bridge through
`PgSession`, plus the PL/pgSQL in-tree extension private-state bridge and
per-session reset callback route through `PgSession`, plus the `postgres.c`
unnamed-statement, interactive-switch, and row-description protocol scratch
bridge through `PgSessionTcopState`, plus transaction callback registration
and SQL backup session-state bridges through `PgSession`, plus the
provider-independent JIT callback cache and LLVM provider-private
type/template/module/context cache through `PgSession`, plus the
`CurrentSession` compatibility pointer bridge through `PgSession`, deferred
connection warning scratch bridge through `PgConnection`, and RI fast-path
xact callback registration guard bridge through `PgSession`, plus the
`TopMemoryContext` pointer-slot bridge through `PgExecution`.

Goal: reduce reliance on thread-local globals so sessions can eventually move
between carriers.

Likely workstreams:

- remaining direct-pointer GUC backing variables into `PgSession` and
  transaction/execution state, extending the GUC-table pointer
  rebind/adoption mechanism where generated GUC records store
  backing-variable addresses.
- remaining memory-context tree ownership/reclamation beyond the current
  pointer-slot bridges, especially full `TopMemoryContext` teardown for
  thread-backed backend exit.
- resource owners beyond the current execution pointers split by
  session/transaction/task.
- statement metadata beyond `debug_query_string` into `PgExecution`.
- interrupt/cancel state beyond the current holdoff and pending-interrupt
  bridges into execution/backend state.
- fd cache into session-owned state plus runtime fd budget.
- cache state either session-owned or explicitly synchronized.

Phase 12 organization rule: `backend_runtime.c` remains the core
runtime/orchestration file for root construction, current-pointer installation,
process/thread symmetry, and top-level adoption/reset orchestration. New
domain-specific accessors and trivial lifecycle helpers should move into
fork-owned adjacent subsystem files, with `check-runtime-lifecycles` updated
to scan those files and `MULTITHREADED_RUNTIME_OWNERS.tsv` extended with the
symbol-level mapping from legacy global to runtime bucket/member/accessor.
The first owner files proving this direction are
`src/backend/utils/cache/backend_runtime_cache.c` for cache/function-manager
accessors and `src/backend/utils/activity/backend_runtime_pgstat.c` for
pgstat/backend-status accessors, followed by
`src/backend/jit/backend_runtime_jit.c` for provider-independent and
LLVM-provider JIT state accessors and
`src/backend/utils/misc/backend_runtime_guc.c` for GUC compatibility backing
variables, then `src/backend/utils/misc/backend_runtime_utility.c` for
backend-local utility/formatting/resource-owner callback accessors and
`src/backend/access/transam/backend_runtime_parallel.c` for backend-local
parallel-query accessors. The storage-owner split moves buffer, fd/storage,
lock-manager, and IPC compatibility accessors into
`src/backend/storage/buffer/backend_runtime_buffer.c`,
`src/backend/storage/file/backend_runtime_file.c`,
`src/backend/storage/lmgr/backend_runtime_lmgr.c`, and
`src/backend/storage/ipc/backend_runtime_ipc.c`. Another split moved
frontend/backend connection compatibility accessors into
`src/backend/libpq/backend_runtime_connection.c`.
Future Phase 12 bucket additions should pick an adjacent owner file first;
adding more code to `backend_runtime.c` should be reserved for root runtime
construction, current-object helpers, and top-level adopt/reset calls.

Validation:

- targeted tests per subsystem;
- process-mode and thread-per-session mode stay working;
- static global report shrinks over time.

Exit gate:

Gate E2 is part of Phase 12 completion. Before leaving Phase 12 and starting
scheduler-aware wait work, close the thread-per-session lifecycle and state
ownership gaps identified in
`MULTITHREADED_THREADING_REVIEW.md`. This gate exists because Phase 13 and
Phase 14 will make backend ownership bugs harder to isolate.

Gate E2 requires:

- threaded backend exit and teardown are safe: normal disconnect, abandoned
  clients, `FATAL`, administrator termination, and worker exit must clean up or
  explicitly account for backend/session/connection/execution memory and
  resources without corrupting later carrier startups;
- the thread-exit path must no longer depend on leaving carrier
  `TopMemoryContext` cleanup unresolved because deletion corrupts later
  carriers, unless the remaining memory is deliberately owned by a documented
  longer-lived runtime object and covered by leak/resource accounting;
- PMChild/thread-backed backend ownership is race-free: postmaster signalling,
  thread exit publication, PMChild reaping, `thread_backend`/logical id
  lookup, and worker notification must have a documented synchronization
  contract with no unsynchronized use-after-free-prone pointer handoff;
- threaded GUC initialization uses a systematic per-session adoption/rebind
  model rather than a growing hard-coded whitelist of options reached by the
  current smokes. The model must cover postmaster/runtime defaults,
  database/role settings, startup options, direct-pointer variables, assign
  hooks, reset/default semantics, and extension/custom GUC behavior expected in
  thread-per-session mode;
- the broad threaded startup serialization gate is removed or narrowed to a
  precisely documented critical section with an explicit removal plan. The
  remaining gate, if any, must not serialize normal post-bootstrap SQL
  execution and must be justified by identified shared state;
- `gmake check-global-lifetimes` is run as a required gate check with the
  checked baseline, and any new mutable global either has an explicit lifetime
  annotation or a deliberate baseline update. The check also enforces the
  Phase 12 runtime-local boundary: core backend/session/execution/connection/
  carrier-local globals must stay in the runtime bridge or a documented
  platform/test shim, so future scattered local globals fail the gate instead
  of silently growing the migration backlog;
- `gmake check-runtime-lifecycles` is run as a required gate check with
  `MULTITHREADED_RUNTIME_LIFECYCLE.tsv`, so every runtime-root field has a
  checked lifecycle row. The checked roots currently include `PgCarrier`,
  `PgBackend`, `PgSession`, `PgConnection`, and `PgExecution`, and any new
  root object added during Phase 12 must be added to the manifest/checker
  before it becomes a migration target;
- lifecycle call lists are made mechanically checkable before the remaining
  teardown work: add root-object bucket definition files, X-macros, or an
  equivalent manifest-driven mechanism so constructor, early-adoption, and
  reset/destroy call lists come from one source of truth, and teach
  `check-runtime-lifecycles` to verify that source against
  `MULTITHREADED_RUNTIME_LIFECYCLE.tsv`;
  this is the next Gate E2 implementation slice before more state migration,
  and checked `.def` bucket files included by the top-level runtime
  orchestration are the preferred first implementation unless the code proves a
  different mechanism is simpler. The same slice should make ordinary
  lifecycle work easier for future agents: add small macros, templates, or
  declarative rule columns for common copied-scalar, zero-reset, whole-bucket
  copy/adopt, and destructor-call cases so new buckets do not require updating
  several handwritten lists by memory. Keep nontrivial destructor and ordering
  semantics handwritten and owner-adjacent. If a later Phase 12 migration
  starts repeating lifecycle boilerplate, pause the migration long enough to
  extend the checked helper/definition mechanism instead of adding another
  manual call-list pattern. Lifecycle bookkeeping friction should be treated
  as a prompt to improve the framework: batch related buckets, add the missing
  helper macro/table rule/checker validation, and then move the batch through
  that checked path rather than landing several narrow one-off lifecycle edits;
- before each remaining large Phase 12 migration batch, review whether the
  lifecycle mechanics should be simplified first. If the next batch would add
  repetitive init/adopt/reset/destroy glue, extend the checked macro,
  `.def`-row, or declarative-rule layer before moving the globals. This is a
  Gate E2 work item because it keeps large-batch state migration fast while
  preserving manifest-checked lifecycle coverage;
- the lifecycle-ergonomics review is a required Gate E2 checkpoint. For each
  boilerplate-heavy Phase 12 batch, document whether the existing
  `PG_RUNTIME_DEFINE_*` macros, checked bucket `.def` files, and lifecycle
  checker rules are sufficient. If they are not, extend the checked mechanism
  before migrating the state, then record the chosen pattern in
  `MULTITHREADED_PHASE12_STATE.md`. The operational checklist for this
  checkpoint is: identify the root object and bucket rows, list the lifecycle
  operations the batch repeats, decide whether an existing checked helper
  covers them, add a reusable helper/table/checker rule first if not, and only
  then move the batch through that path;
- that lifecycle-ergonomics checkpoint must produce an explicit preflight note
  before the next object-state migration or teardown batch begins. The note
  must either name the existing bucket rows/macros/checker rules being reused,
  or name the framework extension landed before the migration. This is meant to
  keep larger Phase 12 batches fast without losing manifest-checked lifecycle
  discipline;
- future lifecycle ergonomics work should prefer reusable checked mechanisms
  over local one-off helpers. The desired shape is one manifest row and one
  checked bucket-definition row per migrated field, with `PG_RUNTIME_DEFINE_*`
  or equivalent macros for routine zero-init, scalar copy/adopt, whole-bucket
  copy/adopt, zero-reset, and destructor-call cases. If a migration cannot be
  expressed clearly through that path, document why and keep the semantic
  cleanup handwritten near the owning subsystem;
- Gate E2 lifecycle ergonomics should keep moving toward a small checked
  action vocabulary rather than long handwritten helper lists. Candidate
  actions include zero-init, zero-reset, copy/adopt, copy/adopt-with-reinit,
  reset-through-initializer, and explicit destroy actions for memory contexts,
  lists, hash tables, sockets, and other owned resources. The checker should
  grow with the vocabulary, including rejecting stale or unexplained `(void) 0`
  lifecycle cells for buckets whose manifest row says they own pointer-like or
  teardown-sensitive state;
- before the next repetitive Phase 12 state batch, decide whether this checked
  action vocabulary should be implemented first. If the batch would require
  multiple nearly identical init/adopt/reset helpers, add the action names,
  `PG_RUNTIME_DEFINE_*` wrappers or equivalent table rules, and
  `check_runtime_lifecycles.pl` validation first, then move the batch through
  that mechanism. The exit criterion is that future agents can add routine
  lifecycle buckets by editing the manifest and bucket-definition row, without
  remembering several parallel call lists;
- the first lifecycle action vocabulary slice is in place:
  `PG_RUNTIME_NOOP` replaces bare no-op expressions in checked bucket
  definitions, and `check_runtime_lifecycles.pl` rejects anonymous `(void) 0`
  cells or unknown `PG_RUNTIME_*` action names. Future vocabulary extensions
  should follow the same pattern: named bucket-row action, C expansion, and
  checker validation;
- `MULTITHREADED_RUNTIME_OWNERS.tsv` remains synchronized with the lifecycle
  manifest and runtime accessors. `check-runtime-lifecycles` must reject owner
  rows that point at a non-manifest bucket, a missing owner source, a duplicate
  legacy symbol, or an accessor not found in the owner source/runtime header;
- the Phase 12 runtime and test scaffolding is refactored before additional
  Gate E2 state migration or Phase 13 scheduler-aware wait work begins.
  `src/backend/utils/init/backend_runtime.c` must remain the orchestration
  file rather than the permanent home for every lifecycle helper/accessor,
  owner-adjacent `backend_runtime_*.c` files must be included in the checked
  runtime source set, and
  `src/test/modules/test_backend_runtime/test_backend_runtime.c` must be split
  into smaller object-focused test sources while preserving the same extension
  and regression surface;
- every backend/session/connection/execution state bucket has an explicit
  lifecycle classification before Phase 12 closes: initializer, early-adoption
  behavior or proof that early adoption is impossible, reset/destroy behavior,
  owner/lifetime, and copy/adoption rule for pointer, list, memory-context,
  socket, hash-table, and opaque-pointer fields. Process/runtime initialization
  and thread-runtime installation must either be mechanically centralized or
  document every intentional asymmetry;
- focused threaded stress covers concurrent startup, idle waits, cancellation,
  termination, SQL `ERROR` recovery, transaction abort cleanup, abandoned
  clients, repeated reconnects, worker launch/shutdown, GUC-heavy sessions,
  and clean fast shutdown;
- process-mode behavior remains the control group, with at least core
  regression coverage and targeted tests for subsystems touched during Phase
  12 cleanup.

Gate E2 maintainability progress: the backend-runtime test extension has been
split by object family while preserving the same extension, SQL regression,
expected output, and TAP entry points. The shared test header is
`src/test/modules/test_backend_runtime/test_backend_runtime.h`; the split
sources are `test_backend_runtime_backend.c`,
`test_backend_runtime_backend_core.c`,
`test_backend_runtime_backend_interrupt.c`, `test_backend_runtime_pmchild.c`,
`test_backend_runtime_session.c`, `test_backend_runtime_connection.c`, and
`test_backend_runtime_execution.c`, with the session GUC half further split
into `test_backend_runtime_session_guc.c`, the small module/launch tests kept
in `test_backend_runtime.c`, and threaded extension entry points kept in
`test_backend_runtime_threaded.c`. The runtime bridge also moved buffer,
fd/storage, lock-manager, IPC, and frontend/backend connection compatibility
accessors into owner-adjacent `backend_runtime_*.c` files, and the lifecycle
checker default source set now includes those split files. The first lifecycle
ergonomics cleanup also added checked `PG_RUNTIME_DEFINE_*` helper macros for
routine zero-init and whole-bucket early fallback copy/adopt functions, so
simple lifecycle boilerplate can move behind macros without weakening manifest
validation. A follow-up GUC split moved the remaining server/runtime,
connection, core registry, miscellaneous, threaded-mutex-depth, and GUC
error-reporting accessors into `backend_runtime_guc.c`, leaving
`backend_runtime.c` with only fallback-aware current-bucket selectors for that
owner. A further session split moved broad session-owned compatibility shims
for namespace, locale, database, tablespace, binary-upgrade, text-search, tcop,
extension, invalidation, RI, relmap, prepared statements, on-commit actions,
and sequences into `backend_runtime_session.c`, while keeping current-bucket
selection centralized. A GUC adoption maintainability slice then replaced the
large handwritten `RebindSessionGUCVariablePointers()` sequence with the
typed `threaded_session_guc_rebinds[]` table and one generic rebind helper,
so future migrated built-in direct-pointer GUCs are one-row additions instead
of another manual `find_option()` block. The backend-runtime regression now
calls `ValidateSessionGUCVariableRebinds()` through
`test_session_guc_rebind_table_matches_registry()`, so stale names, wrong
types, and stale direct-variable pointers fail in the focused runtime test. A
carrier lifecycle checker slice then added `PgCarrier` to the same manifest
discipline: `backend_runtime_carrier_buckets.def` now covers every carrier
field, and `check-runtime-lifecycles` verifies process and thread runtime
construction both call `PgCarrierInitializeRuntimeObject()`.
The owner-map hardening slice also made `check-runtime-lifecycles` validate
`MULTITHREADED_RUNTIME_OWNERS.tsv`, so symbol-level mappings cannot drift away
from checked lifecycle buckets or owner-adjacent accessors.

Current Gate E2 progress: `gmake check-global-lifetimes` is now a required
target, and postmaster signal/wakeup routing no longer dereferences a
thread-backed `PMChild`'s raw `thread_backend` pointer directly. Thread exit
publication now clears the backend pointer, stores the exit status, and wakes
the postmaster through one PMChild helper. The postmaster now treats
successful native thread join as the boundary before PMChild cleanup and slot
release; if `pg_thread_join()` fails, the claimed exit report is restored and
the PMChild remains active for a later retry. Thread exit also reports retained
carrier `TopMemoryContext` bytes to the postmaster reaper as explicit
accounting for the currently retained top context. Backend libpq connection
teardown now frees the frontend/backend wait set and dynamically sized send
buffer in `socket_close()`, and `Port` plus most startup packet/remote-host
strings now live in a dedicated `PortContext` that `socket_close()` deletes
during backend exit. Follow-up work moved the connection authentication
identity, forward-confirmed remote hostname, and implicit reject HBA record
into the same context. SSL/GSS connection-owned identity state now follows the
same lifetime: `pg_gssinfo`, GSS principal strings, and SSL peer certificate
names are allocated in `PortContext`. This removes another concrete
connection-owned allocation group from the retained top-memory bucket before
PMChild exit accounting runs. `AuxProcessResourceOwner` is now stored inside
`PgBackend` behind the existing lvalue compatibility name, with an early
fallback adopted during process/thread runtime installation, so it is no
longer a standalone backend-local TLS global. `MyProc` is now stored inside
`PgBackend` behind a source-compatible lvalue macro and
`PgCurrentMyProcRef()`, with an early fallback adopted during process/thread
runtime installation; the shared-memory `PGPROC` object lifecycle is unchanged,
but the backend-local pointer is no longer standalone TLS. `MyProcNumber` and
`ParallelLeaderProcNumber` now follow the same model through
`PgCurrentMyProcNumberRef()` and `PgCurrentParallelLeaderProcNumberRef()`,
with storage inside `PgBackend`, explicit `INVALID_PROC_NUMBER`
initialization for process and thread runtime state, and early fallback
adoption for pre-runtime writes. The shared-memory `PGPROC`, procarray, and
parallel-worker assignment/release lifecycle remains unchanged. `MyBEEntry`
is now also stored inside `PgBackend` through `PgCurrentMyBEEntryRef()`,
keeping the backend-status shared-memory slot pointer with logical backend
state while leaving `pgstat_beinit()`, `pgstat_beshutdown_hook()`, and the
underlying `PgBackendStatus` shared-memory array lifecycle unchanged.
`MyBgworkerEntry` is now stored inside `PgBackend` through
`PgCurrentMyBgworkerEntryRef()`, keeping background-worker registration
identity with the logical backend while preserving the existing bgworker
registration slot and shared-memory lifecycle.
The opted-in `worker_spi` module no longer keeps its custom wait-event ID in
backend-local TLS; `worker_spi_wait_event_main` is classified as
`PG_GLOBAL_RUNTIME` because it caches a shared wait-event registry ID, not
backend-owned mutable state. A raw scan for
`PG_THREAD_LOCAL PG_GLOBAL_BACKEND`, `PG_GLOBAL_SESSION`,
`PG_GLOBAL_CONNECTION`, and `PG_GLOBAL_EXECUTION` declarations now finds no
matches outside `src/backend/utils/init/backend_runtime.c` early-fallback
storage.
The Windows socket emulation flag `pgwin32_noblock` also now lives in
`PgConnection.socket_io`; this is validated through the shared connection
runtime tests here, with explicit Windows build coverage still required.
The temporary threaded GUC critical-section reentrancy counter now lives in
`PgCarrier` behind `PgCurrentThreadedGUCMutexDepthRef()`, so nested lock depth
is carrier-owned instead of standalone carrier TLS. This does not remove the
temporary process-wide GUC mutex or close the broader GUC adoption/rebind
blocker; it only makes the current bridge's carrier-local state explicit and
covered by backend-runtime tests.
The wait-event signal/self-pipe descriptors, WaitLatch `waiting` flag,
stack-depth base pointer, and opaque backend-thread launch record also now
live in `PgCarrier`, with owner-adjacent accessors in `backend_runtime_ipc.c`,
`backend_runtime_utility.c`, and the core runtime orchestration file. This
preserves the old descriptor sentinels while removing these raw carrier-local
TLS globals from their subsystem files. `IsUnderPostmaster` remains a
deliberate carrier-local process-context flag outside `PgCarrier`.
The generic main-loop interrupt flags `ConfigReloadPending` and
`ShutdownRequestPending` now live in `PgBackendPendingInterruptState` behind
their existing lvalue names, so config reload and cooperative shutdown state
follows the logical backend rather than standalone TLS. The remaining
worker-specific pending flags `WakeupStopPending`,
`AutoVacLauncherPending`, and `CheckpointerShutdownXLOGPending` now follow the
same model, keeping archiver stop wakeups, autovac launcher wakeups, and
checkpointer shutdown-XLOG requests in the logical backend's pending-interrupt
state. The exit in-progress flags `proc_exit_inprogress` and
`shmem_exit_inprogress` now live in `PgBackendExitState` behind compatibility
macros in `storage/ipc.h`, so exit and shared-memory-exit state also follows
the logical backend instead of exported standalone TLS. A larger coherent
pgstat state-family batch now stores `PendingBgWriterStats`,
`PendingCheckpointerStats`, `pgStatBlockReadTime`, `pgStatBlockWriteTime`,
`pgStatActiveTime`, and `pgStatTransactionIdleTime` in
`PgBackendPgStatPendingState` behind `pgstat.h` compatibility macros, removing
six exported backend-local TLS definitions while keeping the existing in-tree
source names. The next pgstat batch moved `PendingIOStats`, `have_iostats`,
`pending_SLRUStats`, `have_slrustats`, `PendingLockStats`,
`have_lockstats`, `pgStatXactCommit`, `pgStatXactRollback`,
`total_func_time`, and `prevWalUsage` into the same
`PgBackendPgStatPendingState` bucket. `PGSTAT_SLRU_NUM_ELEMENTS` now exposes
the fixed SLRU pending-array size needed by the runtime object and is checked
against the internal `slru_names[]` list.
The executor instrumentation counters `pgBufferUsage`, `save_pgBufferUsage`,
`pgWalUsage`, and `save_pgWalUsage` now move as one backend instrumentation
state family into `PgBackendInstrumentationState` behind `instrument.h`
compatibility macros, removing another fixed accounting group from standalone
backend-local TLS.
The backend/fixed pgstat flush state `PendingBackendStats`,
`backend_has_iostats`, `prevBackendWalUsage`, `pgstat_report_fixed`,
`pgStatForceNextFlush`, `force_stats_snapshot_clear`,
`pgstat_is_initialized`, and `pgstat_is_shutdown` now also lives in
`PgBackendPgStatPendingState` behind `pgstat.h` compatibility macros. The
pending-entry context/list state `pgStatPendingContext` and `pgStatPending`
now also lives in that backend-owned pgstat bucket behind private pgstat
accessors/macros. The adoption path asserts that no early pending-entry list
exists before runtime adoption, because copied non-empty `dlist_head` values
would still point at the old list head; after adoption the logical backend owns
a freshly initialized pending-entry list head.
The storage pending/smgr batch now stores `pendingOps`, `pendingUnlinks`,
`pendingOpsCxt`, `sync_cycle_ctr`, `checkpoint_cycle_ctr`,
`sync_in_progress`, `SMgrRelationHash`, `unpinned_relns`, and `MdCxt` in
`PgBackendStorageState` behind private compatibility macros in `sync.c`,
`smgr.c`, and `md.c`. This keeps backend-local fsync/unlink tracking and smgr
relation-cache state with the logical backend; the smgr adoption path asserts
that no early relation hash/list exists before runtime adoption because a
non-empty copied `dlist_head` would still point at the old list head.
The follow-up file-descriptor/VFD batch now stores `VfdCache`,
`SizeVfdCache`, `nfile`, `temporary_files_allowed`, `numAllocatedDescs`,
`maxAllocatedDescs`, `allocatedDescs`, and `numExternalFDs` in the same
`PgBackendStorageState` bucket behind private `fd.c` compatibility macros.
Threaded startup can reserve file descriptors before runtime installation, so
`InstallPgThreadBackendRuntimeState()` now adopts early storage fallback state
along with the other early backend buckets.
The deadlock detector workspace batch now stores `visitedProcs`,
`nVisitedProcs`, `topoProcs`, `beforeConstraints`, `afterConstraints`,
`waitOrders`, `nWaitOrders`, `waitOrderProcs`, `curConstraints`,
`nCurConstraints`, `maxCurConstraints`, `possibleConstraints`,
`nPossibleConstraints`, `maxPossibleConstraints`, `deadlockDetails`,
`nDeadlockDetails`, and `blocking_autovacuum_proc` in `PgBackendLockState`
behind private `deadlock.c` compatibility macros. The runtime state keeps the
private `EDGE`, `WAIT_ORDER`, and `DEADLOCK_INFO` storage opaque, preserving
the local source-file boundary while removing the raw backend-local TLS
workspace.
The local-buffer batch now stores `NLocBuffer`, `LocalBufferDescriptors`,
`LocalBufferBlockPointers`, `LocalRefCount`, `nextFreeLocalBufId`,
`LocalBufHash`, `NLocalPinnedBuffers`, and the `GetLocalBufferStorage()`
allocation cursor/context fields in `PgBackendBufferState`. This removes both
the file-scope local-buffer TLS and the function-local static allocation
cursor that would otherwise be shared by thread backends.
The shared-buffer pin/writeback batch now extends `PgBackendBufferState` with
`BackendWritebackContext`, `PinCountWaitBuf`, the private refcount array/hash
state, and `MaxProportionalPins`. `BackendWritebackContext` remains
object-like at call sites through a `buf_internals.h` compatibility macro,
while the runtime object owns the per-backend storage pointer.
The IPC/sinval batch now stores `MyProcSignalSlot`,
`SharedInvalidMessageCounter`, `catchupInterruptPending`, and the recursive
`ReceiveSharedInvalidMessages()` buffer/cursor state in a new
`PgBackendIPCState` bucket. This keeps proc-signal slot ownership and
already-fetched invalidation state attached to the logical backend rather than
to a carrier thread. The batch passed the clean full build/install,
process-mode backend-runtime regression, direct threaded runtime TAP, contrib
build, and required global-lifetime scan with zero new unclassified mutable
globals.
The lock-manager batch now extends `PgBackendLockState` beyond deadlock
detector workspace to cover fast-path lock-group counters, relation-extension
lock ownership, local lock hash state, strong-lock progress, awaited-lock and
awaited-owner state, the deadlock-timeout pending flag, condition-variable
sleep target, and speculative insertion token state. This removes another
coherent backend-local lock/wait state group from standalone TLS while keeping
the lock-manager source-level API stable. The batch passed the clean full
build/install, process-mode backend-runtime regression, direct threaded
runtime TAP, contrib build, and required global-lifetime scan with zero new
unclassified mutable globals.
The transaction-state batch now stores transaction-status cache state,
two-phase locked-GXACT/exit-registration state, the private two-phase GXACT
lookup cache, SLRU error-report state, and multixact member cache/debug-string
state in a new `PgBackendTransactionState` bucket. This batch intentionally
included function-local statics in `twophase.c` and `multixact.c` that were
not visible as annotated TLS declarations but would still be shared by
thread-backed logical backends. It passed the clean full build/install,
process-mode backend-runtime regression, direct threaded runtime TAP, contrib
build, and required global-lifetime scan with zero new unclassified mutable
globals.
The follow-up ProcArray slice moved the
`TransactionIdIsInProgress()` negative-result cache, `GlobalVisState`
visibility-horizon caches, horizon recomputation throttle, and optional
`XIDCACHE_DEBUG` counters into `PgBackendTransactionState`. The backend
activity/pgstat shared-ref slice then moved the local backend-status snapshot
table pointer, snapshot count, and snapshot memory context into
`PgBackendActivityState`, and moved the pgstat shared-entry reference-cache
pointer, shared-reference age, and reference-cache memory contexts into
`PgBackendPgStatPendingState` behind private pgstat accessors. `pgStatLocal`
remains a dedicated follow-up because its type depends on internal pgstat
snapshot state. Both slices passed clean full build/install, process-mode
backend-runtime regression, direct threaded runtime TAP, contrib build, and
required global-lifetime scans with zero new unclassified mutable globals.
The always-built LWLock state slice moved the held-LWLock count, fixed
held-LWLock handle array, and backend-local user-defined tranche count into
`PgBackendLockState`, preserving the existing `lwlock.c` source names behind
runtime-backed compatibility macros. A follow-up LWLock stats slice moved the
optional `LWLOCK_STATS` debug hash, dummy entry, memory context pointer, and
exit-registration flag into the same backend lock-state bucket. The normal
checkout does not compile the debug-only stats block, so validation combines
normal object/build coverage, runtime accessor tests, and the global-lifetime
scan. The slice passed clean full build/install, process-mode backend-runtime
regression, direct threaded runtime TAP, contrib build, and the required
global-lifetime scan with zero new unclassified mutable globals.
The snapshot/combo-CID execution-state slice moved `snapmgr.c` current,
secondary, catalog, and historic snapshot state, active/registered snapshot
tracking, exported-snapshot tracking, `TransactionXmin`/`RecentXmin`,
`FirstSnapshotSet`, and combo-CID hash/array/counter state into
`PgExecution`. The registered-snapshot heap comparator remains private to
`snapmgr.c`, which lazily initializes the runtime-owned heap. Validation
included touched-object builds, clean full build/install, process-mode
backend-runtime regression, direct threaded runtime TAP, contrib build,
PL/pgSQL rebuild/install, and the required global-lifetime scan with zero new
unclassified mutable globals; execution-local declarations dropped from 154
to 134.
The WAL insert execution-state slice moved `xloginsert.c` registered-buffer
workspace, main-data `XLogRecData` chain state, current insert flags, header
record/scratch storage, registered-data array state, in-progress flag, and
workspace memory context into `PgExecution`. The private `registered_buffer`
type remains local to `xloginsert.c` behind an opaque runtime pointer, and
early adoption retargets the legacy `mainrdata_last` self-pointer sentinel
when needed. Validation included touched-object builds, clean full
build/install, process-mode backend-runtime regression, direct threaded
runtime TAP, contrib build, PL/pgSQL rebuild/install, and the required
global-lifetime scan with zero new unclassified mutable globals;
execution-local declarations dropped from 134 to 121. The hidden
`XLogGetFakeLSN()` function-local statics remain a documented follow-up
because they need a separate session/execution lifetime decision.
The simple transaction execution-state slice moved `XactIsoLevel`,
`XactReadOnly`, `XactDeferrable`, `xact_is_sampled`, `CheckXidAlive`,
`bsysscan`, and `MyXactFlags` into `PgExecution`. `xact.h` preserves those
public names as lvalue macros over runtime accessors without including
`backend_runtime.h`, avoiding a circular header dependency. Validation
included touched-object builds, a clean backend plus `src/common` rebuild,
full `gmake -j8`, install, contrib build, clean PL/pgSQL rebuild/install, the
required global-lifetime scan with zero new unclassified mutable globals, the
test-backend-runtime regression, and the direct threaded runtime TAP;
execution-local declarations dropped from 121 to 108. The private
transaction-state stack, command-id state, timestamps, callback lists, and
abort context remain a separate follow-up because they require a broader
lifecycle split.
The GUC/error scratch-state slice moved GUC check-hook error
code/message/detail/hint state, `pre_format_elog_string()` errno/domain
scratch state, and config-file scanner line/fatal-jump scratch state into
`PgExecution`. `guc.h` keeps the public check-hook string names as lvalue
macros over runtime accessors, while `guc.c`, `elog.c`, and `guc-file.l` use
private compatibility macros for their internal names. Validation included
touched-object builds, a stale-symbol link/load audit that confirmed the
installed-header clean-rebuild requirement, clean backend plus `src/common`
rebuild, full `gmake -j8`, install, clean PL/pgSQL rebuild/install, the
test-backend-runtime regression, contrib build, the required global-lifetime
scan with zero new unclassified mutable globals, and the direct threaded
runtime TAP; execution-local declarations dropped from 108 to 97.
The miscellaneous execution scratch-state slice moved array typanalyze
callback scratch, regex locale scratch, the optional Valgrind command-loop
error counter, and logical-decoding snapshot-builder exported-snapshot scratch
state into `PgExecution`. The moved pointer fields are borrowed or opaque and
the slice uses the same whole-bucket copy/adopt plus zero-reset lifecycle rule
as the adjacent execution scratch buckets. Validation included touched-object
builds, the required global-lifetime scan with zero new unclassified mutable
globals, clean backend plus `src/common` rebuild, full `gmake -j8`, install,
clean PL/pgSQL rebuild/install, contrib build, the test-backend-runtime
regression, and the direct threaded runtime TAP; execution-local declarations
dropped from 97 to 95.
The predicate-lock state slice extends `PgBackendLockState` again for
`predicate.c`: local predicate-lock hash state, the current serializable
transaction pointer, write-tracking flag, and saved serializable transaction
pointer now follow the logical backend through local compatibility macros.
Private `SERIALIZABLEXACT` layout stays private to predicate locking through
opaque runtime pointers. The slice passed touched-object builds, backend
clean/generated-header recovery, clean full build/install, process-mode
backend-runtime regression, a clean threaded runtime TAP rerun, contrib build,
PL/pgSQL rebuild/install, and the required global-lifetime scan with zero new
unclassified mutable globals; backend-local declarations dropped from 58 to
54.
Index-AM WAL redo operation contexts now also live in `PgBackendXLogState`:
the nbtree, GIN, GiST, and SP-GiST redo `opCtx` memory contexts now follow the
logical backend while their owning redo files keep source-local compatibility
macros. The slice passed touched-object builds, backend clean/generated-header
recovery, clean full build/install, process-mode backend-runtime regression,
direct threaded runtime TAP, contrib build, PL/pgSQL rebuild/install, and the
required global-lifetime scan with zero new unclassified mutable globals;
backend-local declarations dropped from 54 to 50.
Memory-manager backend-local state now lives in a dedicated
`PgBackendMemoryManagerState`: allocation-set freelists and the
memory-context logging reentrancy guard now follow the logical backend.
`backend_runtime.h` exposes only the `AllocSetContext` tag, keeping
allocation-set internals owned by `aset.c`. The slice passed touched-object
builds, backend clean/generated-header recovery, clean full build/install,
process-mode backend-runtime regression, direct threaded runtime TAP, contrib
build, PL/pgSQL rebuild/install, and the required global-lifetime scan with
zero new unclassified mutable globals; backend-local declarations dropped from
50 to 49 because the two removed raw globals are offset by one early-backend
fallback bucket.
The backend utility/support state slice now stores dynahash active
sequential-scan tracking, the superuser one-entry cache, the resource-owner
release callback list pointer, and optional `RESOWNER_STATS` lookup counters
in `PgBackendUtilityState`. `ResourceReleaseCallbackItem` remains private to
`resowner.c` through an opaque runtime pointer and file-local typed helper.
The slice passed clean full build/install, process-mode backend-runtime
regression, direct threaded runtime TAP, contrib build, and the required
global-lifetime scan with zero new unclassified mutable globals; backend-local
declarations dropped from 288 to 280.
The follow-up utility-cache slice extends `PgBackendUtilityState` to cover
date/time token caches, degree-trig cached constants, date/time and numeric
format-picture caches, the optional libxml allocation context, and the
missing-attribute datum cache. Private date/time and formatting cache entry
types remain private to their owning source files through opaque runtime
pointer arrays and local casts. The slice passed clean full build/install,
process-mode backend-runtime regression, direct threaded runtime TAP, contrib
build, and the required global-lifetime scan with zero new unclassified
mutable globals; backend-local declarations dropped from 280 to 262.
The parallel/pqmq state slice moves exported parallel worker state, private
parallel context tracking, and private shared-memory message queue redirection
state into `PgBackendParallelState`. Private `FixedParallelState` and
`shm_mq_handle` types stay local to `parallel.c` and `pqmq.c` through opaque
runtime pointers and file-local casts. The early fallback state keeps the
legacy `ParallelWorkerNumber = -1` sentinel statically initialized because
bootstrap consults parallel-worker state before full runtime adoption. The
slice passed clean full build/install, process-mode backend-runtime
regression, direct threaded runtime TAP, contrib build, and the required
global-lifetime scan with zero new unclassified mutable globals; backend-local
declarations dropped from 262 to 249.
The IPC DSM/latch state slice moves `dsm_init_done`,
`dsm_registry_dsa`, `dsm_registry_table`, `LatchWaitSet`, and
`LocalLatchData` into `PgBackendIPCState`. Runtime installation now retargets
adopted early `backend->core.latch` and `backend->interrupt_latch` pointers
from the early fallback latch to the backend-owned latch because threaded
backend startup initializes the local latch before installing the backend
runtime object. The slice passed clean full build/install, process-mode
backend-runtime regression, direct threaded runtime TAP, contrib build, and
the required global-lifetime scan with zero new unclassified mutable globals;
backend-local declarations dropped from 249 to 244.
The timeout scheduler state slice moves the registered timeout table, active
timeout queue, alarm/signal pending flags, firing-target pointers, and
signal-vs-logical delivery mode into `PgBackendTimeoutState`. `timeout.c`
keeps the existing scheduling and firing logic through compatibility macros
over the current backend timeout bucket, while `PgTimeoutParams` is now exposed
by `timeout.h` so `PgBackend` can own the fixed arrays directly. The slice
passed clean full build/install, process-mode backend-runtime regression,
direct threaded runtime TAP, contrib build, and the required global-lifetime
scan with zero new unclassified mutable globals; backend-local declarations
dropped from 244 to 236.
WAL sender backend-local state now lives in `PgBackendWalSenderState`:
exported WAL sender identity and wakeup flags, the physical/logical streaming
cursor, timeline and local sent-pointer state, reply/keepalive timestamps,
streaming shutdown flags, replication command scratch buffers, uploaded
manifest state, logical decoding context, replication command memory context,
and lag tracker now follow the logical backend. The public WAL sender headers
retain the old names as compatibility macros over `PgCurrentWalSenderState()`,
while `walsender.c` uses private macros and a distinct `local_sent_ptr` name
to avoid collisions with the shared-memory `WalSnd.sentPtr` field. The slice
passed clean full build/install, process-mode backend-runtime regression,
direct threaded runtime TAP, contrib build, and the required global-lifetime
scan with zero new unclassified mutable globals; backend-local declarations
dropped from 236 to 202.
Replication receiver and slot backend-local state now lives in
`PgBackendReplicationState`: `MyReplicationSlot`, synchronous replication wait
mode, and WAL receiver connection/file/logstream/wakeup/reply state now follow
the logical backend. Public slot references retain the `MyReplicationSlot`
compatibility name over `PgCurrentReplicationState()`, while `syncrep.c` and
`walreceiver.c` keep their local names through source-file macros. The runtime
initializer preserves the former non-zero sentinels for sync-rep no-wait,
receive-file `-1`, and primary-standby-xmin true. The slice passed clean full
build/install, process-mode backend-runtime regression, direct threaded
runtime TAP, contrib build, and the required global-lifetime scan with zero
new unclassified mutable globals; backend-local declarations dropped from 202
to 193.
Logical replication worker backend-local state now lives in
`PgBackendLogicalReplicationState`: apply worker context/pointers, logical
worker/subscription identity, walreceiver connection, launcher DSA/hash state,
parallel-apply hash/pool/message state, table/sequence sync scratch state,
logical-info barrier cache, and slot-sync shutdown/observed-configuration
state now follow the logical backend. Public logical replication headers keep
the old names as compatibility macros over `PgCurrentLogicalReplicationState()`,
while source-private state uses local macros in the owning files. A follow-up
completion slice also moved the remaining private worker/slot-sync internals
(`lsn_mapping`, `apply_error_callback_arg`, `subxact_data`, and slot-sync
`sleep_ms`) into the same backend-owned state bucket. The runtime header keeps
private logical-replication layouts opaque, using `struct
LogicalRepRelMapEntry *` and `int` storage instead of including
`logicalrelation.h` or `logicalproto.h` from generic backend include paths.
The slices passed clean full build/install, process-mode backend-runtime
regression, direct threaded runtime TAP, contrib build, PL/pgSQL
rebuild/install, and the required global-lifetime scan with zero new
unclassified mutable globals; backend-local declarations dropped first from
193 to 148 and then from 62 to 58 after the completion slice.
Backend WAL/XLog state now lives in `PgBackendXLogState`: local recovery and
insert-permission flags, exported transaction WAL pointers, local redo and
full-page-write caches, cached write/flush result, open WAL segment tracking,
local min-recovery-point copies, checksum state, insertion-lock bookkeeping,
and WAL debug context now follow the logical backend. Public transaction WAL
pointers remain compatibility macros over `PgCurrentXLogState()`. The local
redo pointer uses a distinct `XLogLocalRedoRecPtr` compatibility name in
`xlog.c` to avoid colliding with shared WAL struct fields named `RedoRecPtr`.
The slice passed clean full build/install, process-mode backend-runtime
regression, direct threaded runtime TAP, contrib build, and the required
global-lifetime scan with zero new unclassified mutable globals;
backend-local declarations dropped from 148 to 128.
Backend recovery/startup/standby state now lives in
`PgBackendRecoveryState`: startup interrupt flags, startup-progress timeout
state, local hot-standby and promote-triggered caches, recovery lock hash
pointers, standby timeout flags, and standby conflict wait backoff now follow
the logical backend. `startup.c`, `standby.c`, and `xlogrecovery.c` keep local
compatibility macros over `PgCurrentRecoveryState()`, and the standby backoff
default is shared as `PG_BACKEND_STANDBY_INITIAL_WAIT_US`. The slice passed
touched-object builds, clean full build/install, process-mode backend-runtime
regression, direct threaded runtime TAP, contrib build, PL/pgSQL rebuild, and
the required global-lifetime scan with zero new unclassified mutable globals;
backend-local declarations dropped from 128 to 115.
Backend maintenance-worker state now lives in
`PgBackendMaintenanceWorkerState`: archiver module scratch and queue state,
checkpointer timing/progress state, bgwriter standby-snapshot cache, WAL
summarizer wait/backoff state, and data-checksum worker local flags now follow
the logical backend. The archive-module errdetail ABI remains
source-compatible through `arch_module_check_errdetail_string` as a macro over
`PgCurrentArchModuleCheckErrdetailStringRef()`. The slice passed
touched-object builds, clean full build/install, contrib build, PL/pgSQL
rebuild/install, process-mode backend-runtime regression, direct threaded
runtime TAP, and the required global-lifetime scan with zero new unclassified
mutable globals; backend-local declarations dropped from 115 to 93.
Backend autovacuum state now lives in `PgBackendAutovacuumState`: autovacuum
launcher and worker cost, signal, freeze-age, memory-context, database-list,
Valgrind-preserved array, and worker-info pointer state now follows the
logical backend. The private `avl_dbase` and `WorkerInfoData` layouts remain
private to `autovacuum.c`; the runtime header only forward-declares their
struct tags. The slice passed touched-object builds, clean full build/install,
contrib build, PL/pgSQL rebuild/install, process-mode backend-runtime
regression, direct threaded runtime TAP, and the required global-lifetime scan
with zero new unclassified mutable globals; backend-local declarations
dropped from 93 to 79.
Backend repack leader/worker state now lives in `PgBackendRepackState`: the
leader `DecodingWorker` pointer, exported worker message-pending flag, worker
role flag, current WAL segment, worker DSM segment pointer, and repacked
heap/toast relfile locators now follow the logical backend. The private
`DecodingWorker` layout remains local to `repack.c`, with the runtime header
forward-declaring only its struct tag. The slice passed touched-object builds,
clean full build/install, contrib build, PL/pgSQL rebuild/install,
process-mode backend-runtime regression, direct threaded runtime TAP, and the
required global-lifetime scan with zero new unclassified mutable globals;
backend-local declarations dropped from 79 to 72.
Backend AIO state now lives in `PgBackendAioState`: the current
`PgAioBackend` pointer, AIO method-worker id, and io_uring method context
pointer now follow the logical backend. `pgaio_my_backend` remains a
source-compatible lvalue macro, while the method-worker and io_uring names stay
file-local macros over the backend runtime state. The slice passed
touched-object builds, backend clean/generated-header recovery, clean full
build/install, contrib build, PL/pgSQL rebuild/install, process-mode
backend-runtime regression, direct threaded runtime TAP, and the required
global-lifetime scan with zero new unclassified mutable globals;
backend-local declarations dropped from 72 to 69.
Backend utility command/cache state now also lives in
`PgBackendUtilityState`: async notify pending and exit-registration flags, the
extension sibling cache head, the injection-point callback cache, and the
legacy sampling reservoir state now follow the logical backend. The slice
passed touched-object builds, backend clean/generated-header recovery, clean
full build/install, contrib build, PL/pgSQL rebuild/install, process-mode
backend-runtime regression, direct threaded runtime TAP, and the required
global-lifetime scan with zero new unclassified mutable globals;
backend-local declarations dropped from 69 to 62.
PMChild assignment and slot release now also scrub stale carrier-visible signal
ids and thread-exit payloads before reuse. PMChild thread-exit publication now
captures the exited logical backend id in the exit payload and clears live
`signal_pid` under the same lock as `thread_backend`, so a dead thread is no
longer advertised as signalable while the postmaster can still log and join
the reported exit.
Thread-backed signal-id reads and claimed thread-exit payload reads now also
run through PMChild helper APIs under the PMChild mutex, matching the
publication side instead of reading those fields directly after the exit flag
is claimed. Thread exit now also has an explicit
`PostmasterChildDetachThreadBackend()` boundary: `backend_thread_finish()`
stops publishing the live logical-backend pointer before final exit
publication, while preserving the exited logical id for postmaster reaping and
logging. This is a prerequisite for safe teardown because later signal routing
cannot target a backend that has committed to carrier exit. The
`test_backend_runtime` regression now also has a native-thread PMChild
publication race helper that repeatedly publishes, detaches, publishes exit,
and claims exit reports while reader threads concurrently call signal-id,
interrupt, and wakeup helpers.
Threaded client-socket ownership is now explicit during backend startup:
`pq_init()` marks the launch-time `ClientSocket` copy invalid only after
`Port` owns the descriptor and `socket_close()` is registered, while
`backend_thread_finish()` closes a still-valid copied socket if startup fails
before that handoff.
Threaded startup
now initializes all built-in generated GUC records whose direct backing-variable
pointers are rebound onto `PgSession`/runtime state, replacing the broad
hard-coded startup whitelist with a systematic rebind-adoption pass plus a
small compatibility list for the remaining TLS dummy startup GUCs
(`session_authorization`, `server_encoding`, and `client_encoding`). Threaded
startup also has the full built-in serialized default replay path: threaded
non-EXEC_BACKEND postmasters write and refresh `global/config_exec_params`,
and threaded backends read it after building the per-thread GUC table, so
configured built-in defaults are adopted into the early fallback
session/runtime buckets before runtime installation. Thread-backed auxiliary
loops that consume the logical interrupt mailbox now honor `ProcDiePending`,
so immediate shutdown no longer leaves background writer, checkpointer,
autovacuum launcher, or WAL writer thread carriers waiting for SIGKILL
escalation in the basic threaded shutdown smoke. The temporary threaded
startup serialization gate is now centralized behind an explicit backend-type
policy and has no remaining backend-type users. Regular client backend startup
now bypasses it after a concrete shared-state fix: the recursive
VACUUM/ANALYZE guard moved out of a function-local static and into
`PgExecutionVacuumState`, preventing concurrent sessions from seeing unrelated
ANALYZE activity as recursive vacuum execution. A 32-connection threaded
startup/catalog/temp-table/ANALYZE stress validated the no-gate regular
backend path. Background writer, checkpointer, WAL writer, startup process,
autovacuum launcher/workers, thread-compatible background workers, archiver,
WAL receiver, WAL summarizer, and slot sync worker are worker-specific
narrowings with concrete startup ownership models. Process-model background
workers remain rejected in threaded mode. Thread-compatible dynamic background
workers now use an
explicit thread startup-complete publication path before the postmaster reports
the shared bgworker slot as started, which keeps dynamic waiters from
terminating the worker while `InitProcess()`, `BaseInit()`, or function lookup
are still in progress. The autovacuum launcher narrowing is validated against
the no-database launcher loop, while autovacuum worker narrowing is validated
against a real database-connected autovacuum worker launch and table vacuum
smoke. Startup is additionally validated through threaded normal startup and
crash recovery, archiver, WAL receiver, and WAL summarizer are additionally
validated through their wakeup/progress, streaming, and clean shutdown paths,
and slot sync worker is validated through a threaded physical standby smoke
that synchronizes a failover logical slot from the primary and verifies standby
catalog usability. A broader attempted bypass for additional non-session
auxiliary workers reproduced an abrupt postmaster death during a threaded
`pg_class` catalog scan; later worker-specific fixes and the
`PgExecutionVacuumState` migration removed the remaining startup-gate users,
so future gate reintroduction must be tied to a named shared-state dependency
and concurrent catalog-startup stress. The remaining PMChild and teardown
blockers are full resource cleanup or deliberate long-lived ownership, broader
real-server reaping stress for termination and abandoned-client races, broader
custom/extension GUC semantics, and broader stress coverage for teardown
races. A direct attempt to reset the exiting carrier's `TopMemoryContext`
children after backend cleanup caused an abrupt postmaster exit during a
parallel threaded reconnect smoke, so `TopMemoryContext` reclamation remains a
Gate E2 blocker rather than a safe cleanup path. Follow-up extension-GUC work found
that some generated GUC records are already rebound while the per-thread table
is constructed, so the "changed pointer" pass alone is not a complete startup
initializer. Threaded runtime installation now runs a narrow required
string-GUC bootstrap for `search_path` and `dynamic_library_path`, which lets
threaded sessions use namespace lookup and `LOAD`. A manual threaded
`LOAD`/`SHOW` smoke proved custom extension GUC placeholder conversion across
three sessions, including reuse of an already loaded module. Catalog-writing
DDL then exposed a separate derived-GUC adoption gap: the
`wal_consistency_checking` string GUC was rebound, but its assign-hook-owned
per-session bool array stayed NULL and crashed `XLogInsert()` during threaded
`CREATE TABLE`. The required bootstrap now includes
`wal_consistency_checking`, and the threaded runtime fixture includes a basic
`CREATE TABLE`/`INSERT`/`DROP TABLE` smoke. Follow-up GUC coverage now also
checks database defaults, role defaults, and startup packet `options=-c ...`
in threaded sessions, including direct-pointer GUCs such as `work_mem` and
`default_statistics_target`. The same fixture now also covers built-in
`SET LOCAL` rollback/commit behavior, `RESET` back to database and startup
packet sources, and custom extension GUC `SET LOCAL`/`RESET` semantics after
per-session module initialization. GUC-heavy threaded stress now runs several
simultaneous sessions through repeated built-in direct-pointer GUC updates,
assign-hook GUC updates including `wal_consistency_checking`, transaction-local
overrides, and per-session custom extension GUC values. Concurrent
temp-table/abandoned-client teardown then exposed another required string-GUC
bootstrap gap: `temp_tablespaces` could remain NULL and crash
`PrepareTempTablespaces()` during threaded `CREATE TEMP TABLE`. The required
bootstrap now includes `temp_tablespaces`, and the threaded runtime fixture
adds concurrent abandoned-client and administrator-termination stress that
proves advisory locks are released, terminated backends leave
`pg_stat_activity`, and the server remains usable. The threaded test module
now also has a real `test_backend_runtime_threaded` extension control file and
SQL script, and the threaded runtime fixture exercises `CREATE EXTENSION`,
extension-created C functions, custom-GUC initialization through `_PG_init()`,
and `DROP EXTENSION`. Threaded teardown coverage now also includes a
test-extension helper that raises backend-local `FATAL`, verifies the logical
backend leaves `pg_stat_activity`, and confirms the server remains usable.
After installing the missing local TAP Perl dependency, the threaded runtime
TAP exposed a SIGHUP/default-replay bug where the postmaster serialized a
garbage dynamic-default `client_encoding` value for a late thread-backed IO
worker. `client_encoding` is now the only post-install required-string
compatibility exception, while other session-owned string GUCs are initialized
by scanning the generated built-in GUC table for NULL string backing pointers
inside the installed `PgSession`. Exec/thread config serialization also writes
`client_encoding` through the authoritative encoding state instead of the
generic string backing pointer.
The threaded runtime TAP now also runs a mixed teardown batch in one live
server, combining backend-local `FATAL`, administrator termination, and
abandoned-client exits while verifying logical backend ids leave
`pg_stat_activity`, advisory locks are released, and the server remains usable.
PMChild slot reuse now also scrubs thread-carrier visible payloads under the
same PMChild mutex used by signal-id, interrupt, wakeup, and exit-payload
readers. Full lifecycle resource cleanup and broader real-server PMChild
termination/reaping stress remain Gate E2 blockers before Phase 13.
Follow-up threaded TAP coverage now installs and exercises a representative
contrib set (`hstore`, `pg_trgm`, `btree_gist`, and `pageinspect`) in threaded
mode. That proves extension DDL plus C extension entry points across
types/operators, GiST opclasses, and page inspection. Those modules now
explicitly opt in to the thread-per-session backend model; `pg_trgm` first
moved its custom GUC backing variables to session-local TLS storage. Phase 16
still owns contrib-wide threaded regression, including the modules that need a
broader state/export audit before thread opt-in.
The focused `test_backend_runtime` regression is runnable again as a
process-mode validation control for runtime-state, state-migration, and
PMChild helper coverage after fake thread-runtime tests were changed to
construct thread backend state without installing it into the active SQL
backend.

Phase 16 still owns broader hardening such as sanitizer runs, contrib-wide
threaded regression, crash/FATAL behavior matrices, platform coverage, and
performance baselines. Gate E2 is narrower: it blocks further scheduler work
until the current thread-per-session runtime has coherent lifecycle, state, and
startup ownership.
Follow-up object-model review also makes the lifecycle audit itself a Gate E2
hardening blocker: the large `PgBackend` bucket is acceptable as a Phase 12
bridge, but it is not a final ownership model until initialization, adoption,
teardown, and pointer/list-bearing copy rules are explicit for each bucket,
and the `PgSession`/legacy `Session` endpoint is documented.
Follow-up Gate E2 hardening centralized backend early fallback adoption in
`PgBackendAdoptEarlyState()`, so process runtime initialization and thread
backend installation no longer maintain separate backend adoption lists. This
explicitly brings WAL sender, replication, logical replication, XLog,
recovery, maintenance-worker, autovacuum, repack, AIO, pending-interrupt, and
interrupt-holdoff adoption into the thread-install path. Backend exit state
remains deliberately separate because it is owned by the backend-exit cleanup
lifecycle. The same slice fixed one pointer/list-bearing copy rule by
asserting that the early autovacuum database list is empty and reinitializing
the adopted backend's list head. Validation included touched-object builds,
the `test_backend_runtime` regression, direct threaded TAP, full `gmake -j8`,
contrib build, and `gmake check-global-lifetimes` with zero new unclassified
mutable globals.
Follow-up hardening centralized the matching session and execution adoption
lists in `PgSessionAdoptEarlyState()` and `PgExecutionAdoptEarlyState()`.
Process runtime initialization and thread backend installation now use those
helpers instead of parallel manual lists, and
`test_thread_install_adopts_session_execution_fallback_state()` covers
representative session/execution fallback adoption plus fallback reset.
Follow-up hardening centralized connection fallback adoption in
`PgConnectionAdoptEarlyState()`. Process runtime initialization passes no
preserved port, while thread backend installation passes the constructor
provided `Port` so the live frontend connection survives adoption.
`test_thread_install_adopts_connection_fallback_state()` covers representative
connection fallback adoption plus fallback reset. Validation included
touched-object builds, the backend-runtime regression, direct threaded TAP,
full `gmake -j8`, contrib build, and `gmake check-global-lifetimes`.
Follow-up connection teardown hardening added
`PgConnectionResetClosedState()`. `socket_close()` still owns freeing the
palloc-backed send buffer and frontend/backend `WaitEventSet`, while the
runtime helper scrubs the retained `PgConnection` socket/protocol/startup/
security buckets and frees the malloc-backed GSS buffers. Follow-up connection
startup cleanup moved deferred connection warning list cells and message/detail
strings into `PgConnection.startup.connection_warning_context`, so normal
warning emission and retained connection reset no longer depend on
`TopMemoryContext` allocations for that path. This closes concrete Gate E2
reset/destroy rules for connection state, but the complete
backend/session/connection/execution destructor tree and `TopMemoryContext`
ownership model remain Phase 12 blockers.
Follow-up session teardown hardening added `PgSessionResetClosedState()`.
`dfmgr.c` now allocates the per-session dynamic-library `_PG_init()` replay
list under `PgSession.dynamic_library_context` instead of `TopMemoryContext`,
and backend exit deletes that context after `on_proc_exit` callbacks have had
their chance to use session state. This closes one concrete list-bearing
`PgSession` reset/destroy rule. Follow-up bridge hardening moved the legacy
`access/session.h` payload allocation behind `PgSessionGetLegacySession()`,
records the dedicated `PgSession.legacy_session_context` in the lifecycle
manifest, and deletes that context during `PgSessionResetClosedState()` after
DSM/DSA detach paths have run. A matching execution cleanup slice now clears
the retained `PgExecution.memory_contexts` slots at the end of backend-exit
cleanup, after session/backend reset still has usable memory-context state.
Follow-up session cache teardown now also drops prepared statements and
destroys the prepared-query hash, frees any leftover `ON COMMIT` action list,
and destroys the remaining async local-channel hash after proc-exit async
callbacks have had their chance to clean shared listener state. These buckets
no longer depend on resetting the whole carrier `TopMemoryContext`.
There are no `GateE2 pending` lifecycle manifest rows left; the broader
`TopMemoryContext` ownership split remains tracked as a separate memory
ownership problem rather than an unclassified bucket.
The next state-migration batch moved catalog transaction/execution scratch
state into `PgExecutionCatalogState`: uncommitted enum type/value hash
pointers, REINDEX suppression state, and pending smgr relation delete/sync
state. This removed seven raw `PG_GLOBAL_EXECUTION` declarations while keeping
the existing enum/reindex/smgr transaction cleanup ownership intact. The
global-lifetime scan now reports 88 execution-local declarations, down from
95, with zero new unclassified mutable globals.
The following state-migration batch moved LISTEN/NOTIFY transaction scratch
state into `PgExecutionAsyncState`: pending LISTEN/UNLISTEN actions, pending
NOTIFY lists, pending listen intent hash state, queue head snapshots used by
`SignalBackends()`, and preallocated signal workspace arrays. Existing async
transaction cleanup and transaction memory contexts still own the pointed-to
list/hash storage, while the runtime object owns the execution-local pointer
slots and queue-position values. The global-lifetime scan now reports 81
execution-local declarations, down from 88, with zero new unclassified mutable
globals.
The next transaction-state batch moved another coherent `xact.c` scalar and
pointer group into `PgExecutionXactState`: top full XID, parallel-current-XID
borrowed pointer and count, inline unreported-XID storage, subtransaction and
command ID counters, transaction/statement/stop timestamps, prepare GID,
force-sync flag, and transaction abort context pointer. The private
`TransactionStateData` stack and transaction callback lists deliberately
remain in `xact.c` for a later lifecycle split, but the moved fields now have
explicit manifest copy/adoption rules. The global-lifetime scan now reports
67 execution-local declarations, down from 81, with zero new unclassified
mutable globals.
The following transaction-cleanup batch moved large-object descriptor cleanup
slots, the transaction temporary-file cleanup flag, the pgstat subtransaction
stack pointer, and the RI fast-path batch-cache pointer/callback flag into
`PgExecutionTransactionCleanupState`. This keeps existing large-object,
temporary-file, pgstat, and RI transaction cleanup authoritative for the
pointed-to storage, while the runtime object owns the execution-local slots
and scalar flags. The lifecycle manifest records the borrowed-pointer rules
and centralized early fallback adoption through `PgExecutionAdoptEarlyState()`.
The global-lifetime scan now reports 60 execution-local declarations, down
from 67, with zero new unclassified mutable globals.
The following execution-scratch batch moved `elog.c`'s error-data stack and
timestamp cache into `PgExecutionErrorState`, and event-trigger query state,
replication-origin transaction state, logical apply error-context stack,
logical apply message context, and logical streaming context into
`PgExecutionReplicationScratchState`. The moved pointer slots remain borrowed
from their existing error, event-trigger, and logical-apply cleanup paths,
while replication-origin transaction state is copied scalar state. The
lifecycle manifest records the copied-scalar and borrowed-pointer rules, and
centralized early fallback adoption reaches the bucket through
`PgExecutionAdoptEarlyState()`. The global-lifetime scan now reports 47
execution-local declarations, down from 60, with zero new unclassified mutable
globals.
The following catalog-cache batch moved catcache's create-in-progress stack
and relcache's build-in-progress list, EOXact relation OID list, and EOXact
tupledesc array slots into `PgExecutionCatalogCacheState`. The runtime object
owns the slots and inline OID array; the pointed-to catcache stack entries,
relcache in-progress list, and tupledesc array remain borrowed from existing
stack, `CacheMemoryContext`, and relcache EOXact cleanup ownership. The
global-lifetime scan now reports 38 execution-local declarations, down from
47, with zero new unclassified mutable globals.
The lifecycle audit is now also mechanically checked. The root
`MULTITHREADED_RUNTIME_LIFECYCLE.tsv` manifest records owner/lifetime,
initializer, early-adoption, reset/destroy, and copy/adoption rules for every
field currently declared in `PgBackend`, `PgSession`, `PgConnection`, and
`PgExecution`. `gmake check-runtime-lifecycles` parses
`backend_runtime.h` and fails if a field is missing from the manifest or if
the manifest contains a stale entry. This makes the Gate E2 bucket-lifecycle
audit enforceable; any future unknown or pending lifecycle row is a Phase 12
blocker until it is resolved or deliberately documented as a long-lived owner.
Follow-up Gate E2 hardening made process-mode runtime setup use the same
backend/session/connection/execution object constructor helpers as threaded
backend-state setup. The lifecycle checker now also verifies manifest-
referenced runtime lifecycle function names against the checked runtime
sources and asserts that process/thread construction and installation retain
the required constructor/adoption calls. This keeps the adoption-list
symmetry from regressing while `PgBackend` remains a Phase 12 consolidation
bridge rather than the final subsystem ownership boundary.
The following session-cache batch moved the text-search parser, dictionary,
and configuration cache hashes plus their last-used entry pointers into
`PgSessionTextSearchState`, alongside the already migrated
`default_text_search_config` value and OID cache. The reset path now destroys
the parser/config hash tables, dictionary private memory contexts, config map
arrays, and last-used pointers explicitly, and the lifecycle manifest records
the pointer/hash ownership rule. The global-lifetime scan now reports 191
session-local declarations and 30 execution-local declarations with zero new
unclassified mutable globals.
The following user-identity cache batch moved `acl.c`'s role-membership cache
arrays and database hash into `PgSessionUserIdentityState`. `acl.c` keeps its
historic local names through file-local macros over the current session
object, while `PgSessionResetClosedState()` now invalidates cached role OIDs
and frees the copied membership lists. The global-lifetime scan now reports
188 session-local declarations and 30 execution-local declarations with zero
new unclassified mutable globals.
The following function-manager cache batch moved `fmgr.c`'s external C
function lookup hash behind `PgSessionFunctionManagerState`. `fmgr.c` keeps
the historic `CFuncHash` name through a file-local macro over the current
session object, and `PgSessionResetClosedState()` now destroys the hash while
leaving dynamic library handles runtime-owned. The scanner count remains 188
session-local declarations because the standalone TLS hash was replaced by the
early fallback session bucket, but the cache itself is now a checked
object-owned field and the runtime lifecycle manifest now classifies 136
fields.
The following invalidation-callback batch moved the syscache, relcache, and
relsync callback registries into `PgSessionInvalidationCallbackState`.
`inval.c` now routes its existing registry names through the current session,
and `PgSessionResetClosedState()` clears callback registrations after
dependent session caches have been destroyed. This makes future cache
migrations safer because callbacks no longer silently survive a logical
session close. The runtime lifecycle manifest now classifies 137 fields.
The same hardening pass fixed the threaded postmaster notification latch
handoff in `launch_backend.c`: the thread-start payload now records the
postmaster latch after runtime-state initialization, falls back to the current
local latch data if `MyLatch` is not populated through the runtime wrapper, and
asserts the published latch is non-NULL before carrier creation. This preserves
the PMChild startup/exit wakeup path used by threaded workers.
The next session datetime batch moved the active timezone-abbreviation table
pointer and timezone-abbreviation lookup cache into `PgSessionDateTimeState`.
The table pointer is a borrowed GUC extra value, while the inline cache is
session scratch reset by the existing timezone-abbreviation and timezone
assign paths. This removes two more session TLS declarations while keeping the
lifecycle rule explicit in `MULTITHREADED_RUNTIME_LIFECYCLE.tsv`.
The following logical-replication session-cache batch moved replication-origin
session state, logical relation/partition map roots, `pgoutput` relation sync
state, and sync-worker relation validity into `PgSessionLogicalReplicationState`.
The bucket has null-asserted early adoption and explicit closed-session cleanup
for owned contexts/hashes; replication-origin refcount release remains owned by
the origin subsystem reset/exit path.
The following catalog-lookup cache batch moved attribute-options,
relfilenumber, tablespace-options, event-trigger, ruleutils SPI-plan, and ICU
converter cache roots into `PgSessionCatalogLookupState`. The reset path now
destroys the owned hash/context/plan/converter roots it can safely own today,
while the lifecycle manifest explicitly records that pointed allocations under
`CacheMemoryContext` remain part of the broader memory-context ownership split.
The following PL/pgSQL in-tree extension batch moved PL/pgSQL's custom-GUC,
compile, namespace, plugin, simple-expression, and cast-cache session state
behind an opaque `PgSessionExtensionModuleState` private pointer. A per-session
reset-callback list now lets PL/pgSQL release its private roots before
`dynamic_library_context` is deleted, establishing the intended in-tree route
for extension-owned session state without exposing PL/pgSQL internals in core.
The following tcop session-state batch moved `postgres.c`'s unnamed prepared
statement pointer, interactive command-line switches, and reused row-description
message context/buffer into `PgSessionTcopState`. Early fallback adoption covers
the `-E`/`-j` switches parsed before a session exists, while session-close reset
drops leftover unnamed plans and deletes the row-description context.
The following session utility-state batch moved `xact.c`'s xact/subxact
callback registration list heads and SQL backup state from `xlogfuncs.c` plus
`xlog.c`'s session backup status into `PgSession`. Session-close reset now
frees leftover callback list nodes and aborts/deletes leftover SQL backup
state explicitly.
The following session cache/flag batch moved RI trigger cache roots and
valid-list state, `debug_discard_caches`, loaded relation-map shared/local
files, and `update_process_title` into `PgSession`. Active/pending relmap
transaction update files remain execution-owned. The global-lifetime scan now
reports 149 session-local declarations, down from 157, with zero new
unclassified mutable globals, and the lifecycle manifest now classifies 145
runtime fields. Validation included touched-object builds, clean full build,
install, contrib build, backend-runtime regression, direct threaded runtime
TAP, `gmake check-runtime-lifecycles`, `gmake check-global-lifetimes`, and
`git diff --check`.
The following central GUC-registry batch added `PgSessionGUCState`, moving
`GUCMemoryContext`, the per-session copied GUC records, the GUC hash table,
non-default/stack/report list heads, reporting state, and `GUCNestLevel` into
`PgSession`. Early adoption now transfers the GUC owner bucket before
GUC-backed string buckets such as datetime, text search, and connection state,
so copied string pointers retain the correct session owner. The batch also
retargets moved dlist/dclist heads instead of shallow-copying fallback self
pointers, covering both GUC non-default state and the existing RI valid-entry
dclist. Detached early string buckets are left uninitialized and NULL after the
GUC owner transfer, avoiding both new fallback GUC-context allocations and
later frees of non-owned fallback strings during partial runtime installation.
Threaded TAP validation exposed an adjacent
teardown hazard in the backend memory-manager freelist bucket; threaded reset
now clears the freelist bucket without walking retained context headers until
full `TopMemoryContext` reclamation lands, while process-mode reset keeps the
destructive freelist free. Threaded mode now also uses a temporary
process-wide GUC critical section around session GUC setup, mutation, and
display while copied GUC metadata, check hooks, assign hooks, and show hooks
still contain process-era assumptions; later phases should narrow it as
remaining GUC-backed globals become session-owned. Threaded
`read_nondefault_variables()` also skips `PGC_POSTMASTER` and `PGC_INTERNAL`
records, because thread carriers share the postmaster address space and must
not replay process-global strings through a session GUC context. The same
validation tested a broader startup serialization gate, but an unconditional
`backend_thread_entry()` gate was rejected because it can block normal threaded
startup behind worker paths that have not reached
`ThreadedBackendStartupComplete()`. Startup serialization is now
helper-controlled behind `backend_thread_requires_startup_gate()` and requires
a named shared-state dependency plus a release/stress test for any backend type
that opts in. Early fallback state, GUC replay, runtime installation, backend
initialization, and worker initialization remain explicit Gate E2 audit targets
rather than being hidden behind a process-wide startup lock. The same follow-up
validation made `CurrentPgRuntime` a carrier/thread-local
current binding, so threaded backend installation no longer changes the
postmaster's runtime view, and moved runtime-global reserved GUC prefixes out
of session `GUCMemoryContext` storage into a `TopMemoryContext` child guarded
by the temporary GUC lock. This keeps PL/pgSQL prefix reservation valid after
threaded backend FATAL cleanup. Subsequent session-cache batches moved portal
manager roots, compiled-regexp cache roots, syscache root arrays, the catcache
header, relcache root hashes/flags/counters, and typcache root
hashes/stacks/counters behind `PgSession`. The global-lifetime scan reported
112 session-local declarations after that batch, down from 149 at the central
GUC slice and down from 123 before the typcache root batch, with zero new
unclassified mutable globals. The lifecycle manifest now classifies 147
runtime fields.
Follow-up cache work moved the exported `CacheMemoryContext` pointer slot
under `PgSessionCatalogLookupState` and kept the historical name as an lvalue
macro through `PgCacheMemoryContextRef()`, implemented in
`backend_runtime_cache.c`. This closes the most immediate cache-context
ownership gap for the moved syscache/catcache/relcache/typcache roots. Active
backend teardown clears the slot but deliberately does not delete the live
cache context; the broader `TopMemoryContext` ownership and full cache-entry
destructor audit remain Gate E2 blockers. Follow-up cache work moved
`funccache.c`'s cached-function hash root into
`PgSessionFunctionManagerState`, with the compatibility accessor in
`backend_runtime_cache.c` and the tuple-descriptor/language-callback destructor
handwritten in `funccache.c`. Follow-up JIT work moved the
provider-independent callback/load-status cache into `PgSessionJitProviderState`
and then moved the LLVM provider-private type/template/module/context cache
into `PgSessionLLVMJitState` under an LLVM-enabled build. The LLVM smoke forced
zero JIT thresholds and produced leader/parallel-worker JIT functions after
fixing the generated-IR memory-context switch to call
`PgCurrentMemoryContextRef()` instead of resolving the removed
`CurrentMemoryContext` global. The global-lifetime scan now reports 61
session-local declarations with zero new unclassified mutable globals.

## Phase 13: Scheduler-Aware Wait Boundary

Goal: extend the visible wait boundary so sessions can suspend and resume
without pinning an OS thread.

Likely changes:

- Convert `PgSuspend()` waits from "block current carrier" to "register wait and
  yield carrier" where safe.
- Add wait completion records that can requeue the owning session/execution.
- Make frontend input, frontend output, latch, lock, condition variable, and
  timeout waits scheduler-visible.
- Keep thread-per-session mode available with blocking waits for debugging and
  fallback.

Validation:

- process-mode tests pass;
- thread-per-session tests pass;
- cancellation while blocked still works;
- idle timeout and transaction timeout behavior remains correct;
- no lost wakeups in common wait paths.

## Phase 14: Pooled Carrier Scheduler

Goal: schedule logical session/execution tasks onto a smaller number of worker
threads.

Likely changes:

- Add runnable queues and wait queues.
- Add task budget handling.
- Make `PgSessionStep()` return when it reaches visible waits.
- Move blocking waits to scheduler registration.
- Wake blocked tasks through logical interrupts or wait completion.
- Keep thread-per-session mode available for debugging and fallback.

Initial limitations are acceptable:

- long executor calls may pin a carrier until they reach safe yield points;
- some subsystems may temporarily opt out and require a dedicated carrier;
- threaded worker families may temporarily require dedicated carriers before
  they become safe to schedule on the pooled carrier set.

Validation:

- many idle sessions on few carriers;
- many clients waiting on locks without many blocked OS threads;
- no lost wakeups under stress;
- cancellation of waiting and running tasks;
- process-mode and thread-per-session modes still work.

Exit gate:

- Gate F is part of Phase 14 completion. Before leaving Phase 14, run the Gate
  F checks from the Test Strategy section: full process-mode and threaded-mode
  suites plus stress tests for lock waits, cancellation of waiting and running
  tasks, output backpressure, timeout delivery while waiting, and lost wakeups.

## Phase 15: Executor And Utility Yield Points

Goal: improve fairness and latency for long-running commands under pooled
scheduling.

Likely changes:

- add safe yield checks in long executor loops;
- add batch boundaries in sort/hash/materialize paths where safe;
- add COPY yield points;
- add utility command yield points for long operations;
- integrate with interrupt checks and budget accounting.

Validation:

- long query fairness tests;
- cancellation latency tests;
- no executor state corruption across yields;
- performance comparison against thread-per-session mode.

## Phase 16: Contrib Extension Completion And Hardening

Goal: make threaded mode debuggable, credible, and complete for in-tree
extensions, including contrib.

Likely work:

- migrate every contrib extension to explicit backend model metadata;
- make every contrib extension support thread-per-session mode by default;
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

- Gate G is part of Phase 16 completion and may need to run repeatedly during
  hardening. Before considering Phase 16 complete, run the Gate G checks from
  the Test Strategy section: feasible sanitizers, repeated full suites,
  threaded contrib regression for every contrib extension, stress tests,
  crash/FATAL behavior tests, and performance baselines.

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

Gate F, after Phase 14:

- scheduler-aware waits and pooled carriers exist;
- run full process-mode and threaded-mode suites, plus stress tests for lock
  waits, cancellation of waiting and running tasks, output backpressure,
  timeout delivery while waiting, and lost wakeups.

Gate G, during and before completing Phase 16:

- hardening and release-readiness gate;
- run sanitizers where feasible, repeated full suites, threaded contrib
  regression tests for every contrib extension, stress tests for
  interrupts/waits/cancellation/teardown, crash and `FATAL` behavior tests, and
  performance baselines.

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
- provide session-state APIs;
- migrate PL/pgSQL and selected in-tree modules first.

### `PGPROC` Ownership

Risk: `PGPROC` currently conflates backend identity, lock waiting, proc array
membership, and transaction visibility.

Mitigation:

- in early thread-per-session mode, keep one `PGPROC` per logical backend;
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

## Suggested Commit Sequence After Documentation

1. Land a minimal process-mode `PgSessionStep()` boundary.
2. Add runtime/session/backend object declarations and process-mode no-op
   initialization.
3. Connect current process-mode startup to the object skeleton.
4. Attach main-loop state to `PgSession`.
5. Extract session bootstrap from `PostgresMain()`.
6. Make the protected `PgSessionStep(PgSession *, PgStepBudget)` contract
   explicit.
7. Add global lifetime annotation macros and initial static report tooling.
8. Annotate globals needed for the thread-per-session safety floor.
9. Introduce logical interrupt structs and bridge signal handlers to them.
10. Convert timeout delivery to target logical backends.
11. Split backend exit cleanup from process exit.
12. Add extension backend model metadata and process-only default.
13. Add test-only threaded backend model checks for extension loader policy.
14. Establish the minimum in-tree module allowlist.
15. Make the thread-safety floor private through TLS or object ownership.
16. Add the thread-compatible wait/wakeup boundary.
17. Add thread portability layer and backend launch switch.
18. Run first thread-per-session backend smoke tests.
19. Add the auxiliary worker runtime owner and threaded worker launch path.
20. Migrate in-tree auxiliary worker families to threaded carriers.
21. Migrate TLS-backed state toward object-owned state.
22. Convert scheduler-aware waits from blocking to suspend/resume.
23. Add pooled carrier scheduling.
24. Add executor and utility yield points.
25. Migrate all contrib extensions to threaded-mode metadata and tests.
26. Harden with sanitizers, stress tests, repeated full suites, and performance
    baselines.

Each commit should leave process mode buildable. Prefer temporary compatibility
wrappers to broad all-at-once rewrites.
