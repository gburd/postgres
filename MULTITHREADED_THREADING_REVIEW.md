# Multithreaded PostgreSQL Branch Review

This report reviews the current threaded PostgreSQL implementation against the
architecture and staged plan. It focuses on whether the branch is converging on
a real thread-per-session PostgreSQL runtime or accumulating proof-of-concept
shortcuts that will become correctness debt.

The review was based on repository documentation and read-only source
inspection. No build, test, server, or mutation commands were run as part of
the review because another agent was working concurrently in the checkout.

## Executive Assessment

The branch is directionally aligned with the design. It is not merely a toy
`pthread_create()` wrapper. The implementation includes substantive work:

- explicit `PgRuntime`, `PgCarrier`, `PgBackend`, `PgSession`,
  `PgConnection`, and `PgExecution` objects;
- a protected stepped session loop;
- logical backend ids distinct from OS pids;
- thread-backed regular client backends;
- initial logical interrupt, timeout, procsignal, and latch wakeup paths;
- extension backend-model gating;
- thread-carrier support for many in-tree worker families;
- large-scale migration of backend/session/execution state from process
  globals and TLS into runtime-owned objects.

However, the branch should not currently be treated as a finished threaded
runtime. It is best described as a serious prototype with real architecture,
but with several correctness blockers that must be resolved before scheduler
work, pooled carriers, or broad worker/contrib claims can safely build on it.

The most important distinction is this:

- TLS as a thread-per-session bridge is acceptable and matches the plan.
- Incomplete lifecycle ownership, hard-coded per-path GUC adoption, broad
  startup serialization, and unsynchronized thread/postmaster pointers are not
  just cleanup. They are correctness blockers.

If Phase 12 and the new Phase 12 exit gate force those issues closed, the
branch can continue toward a real multithreaded PostgreSQL. If those issues
are deferred to generic Phase 16 hardening, the branch risks becoming a
fragile proof of concept whose smokes pass only along curated paths.

## Positive Signals

### Object Model Is Taking Shape

The implementation has moved beyond annotations and has real ownership
objects. Current runtime/backend/session/connection/execution pointers exist,
and many legacy globals now route through compatibility accessors backed by
those objects.

This is aligned with the architecture's central goal: make backend-local and
session-local state explicit before attempting pooled scheduling.

### Main-Loop Refactor Matches The Plan

`PgSessionStep()` is a protected public boundary with error recovery, and
`PgSessionRun()` loops over that boundary. This preserves PostgreSQL's
`ERROR`/`FATAL` semantics while creating a future scheduler entrypoint.

That is the correct direction. The branch has not prematurely rewritten the
executor into callbacks or return-code style.

### Thread Launch Is Real

The branch has a native thread portability layer and starts backend carriers
as OS threads in threaded mode. PMChild gained thread-carrier state, thread
exit is reaped by the postmaster, SQL-visible backend ids can be logical ids,
and the client protocol path can execute real SQL in thread-backed sessions.

This is more than scaffolding.

### Interrupt And Wakeup Work Is Meaningful

Logical interrupt types, backend mailboxes, procsignal integration, same-
process thread wakeups, logical timeout clamping, and latch wakeups are all
visible. The implementation is no longer relying solely on Unix signals for
same-address-space backend communication.

### Extension Gating Is The Right Safety Policy

The default process-only extension model and explicit thread-per-session
metadata are the right compatibility stance. Existing arbitrary C extensions
cannot be assumed thread-safe.

### Cache State Is Treated Conservatively

Relcache, catcache, syscache, typcache, and related caches are mostly being
kept per-session/per-execution through TLS or object bridges. That is
conservative and consistent with the plan's thread-per-session milestone.
Sharing these caches too early would be riskier.

## Major Findings

### 1. Threaded Backend Teardown Is Not Yet Correct

Severity: High

The thread exit path explicitly avoids deleting the carrier's
`TopMemoryContext` because doing so corrupts later carrier startups. That is a
serious ownership gap.

Why it matters:

- Process backends rely on process exit for final memory cleanup.
- Threaded backends cannot rely on process exit without leaking or corrupting
  the long-lived postmaster/runtime address space.
- A backend that exits normally, exits after `FATAL`, is terminated by an
  administrator, or loses its client must be able to release backend/session/
  execution resources without damaging later carriers.

This cannot be postponed to generic hardening. It is a Phase 12 correctness
requirement.

Required resolution:

- define what memory belongs to the carrier, backend, session, connection, and
  execution;
- make threaded backend exit run cleanup to a stable post-cleanup state;
- either safely delete per-carrier/per-backend memory contexts or document and
  account for intentional long-lived runtime ownership;
- add stress tests that repeatedly start, terminate, abandon, and reconnect
  threaded clients while checking memory/resource cleanup.

### 2. PMChild And Thread Backend Pointer Ownership Looks Racy

Severity: High

The postmaster stores a pointer to a thread-backed `PgBackend` in PMChild so it
can route signals and wakeups. The backend thread later clears that pointer and
frees the thread start record. The inspected code does not show a clear
lock/atomic ownership protocol around these cross-thread accesses.

Why it matters:

- the postmaster can signal a thread-backed child during shutdown;
- a worker can exit concurrently with postmaster signal routing;
- stale PMChild backend pointers can become use-after-free bugs;
- data races in the postmaster control plane are especially dangerous because
  they can corrupt the whole runtime.

Required resolution:

- establish one owner for PMChild thread fields;
- publish and clear thread-backend pointers under a documented synchronization
  protocol;
- avoid direct unsynchronized pointer reads from the postmaster;
- prefer stable logical ids plus a locked/atomic registry lookup where
  possible;
- test shutdown, worker termination, normal thread exit, abnormal thread exit,
  and concurrent postmaster signalling.

### 3. Threaded GUC Initialization Is Still A Whitelist Bridge

Severity: High

`InitializeThreadedSessionGUCOptions()` initializes a manually curated list of
GUC records that the current threaded startup and smoke paths happen to reach.
The code comments still describe this as a narrow bridge that must be replaced
before arbitrary SQL can run, but the branch now permits arbitrary threaded
sessions.

Why it matters:

- newly reached SQL paths can dereference uninitialized thread-local GUC
  records;
- path-dependent crashes have already been found and fixed by adding one more
  GUC to the whitelist;
- custom and extension GUC behavior remains especially fragile;
- this creates a maintenance pattern where every new failure adds another
  one-off bridge.

Required resolution:

- replace the growing whitelist with systematic per-session GUC table
  initialization/adoption;
- define how postmaster/runtime defaults become session defaults in threaded
  carriers;
- make direct-pointer GUC rebinding complete and auditable;
- ensure assign hooks, reset/default semantics, database/role settings,
  startup options, transaction nesting, and extension custom GUCs have a
  coherent model;
- add tests that exercise broad GUC families in threaded sessions, not only
  GUCs needed by the current smoke.

Status update: subsequent Phase 12 work replaced the broad hard-coded
`InitializeThreadedSessionGUCOptions()` name list with a systematic pass over
the generated built-in GUC table. The pass records each direct backing-variable
pointer after `InitializeGUCVariablePointers()`, rebinds the table onto the
current logical session/runtime state, and initializes every record whose
backing pointer changed. A small compatibility list remains for TLS dummy
startup GUCs that do not yet have `PgSession` accessors:
`session_authorization`, `server_encoding`, and `client_encoding`. The
remaining Gate E2 GUC work is postmaster/runtime default adoption, full
custom/extension GUC behavior, broader assign-hook/reset/default semantics,
and threaded stress coverage for GUC-heavy sessions.

Further status update: threaded non-EXEC_BACKEND postmasters now write and
refresh `global/config_exec_params` when `multithreaded` is enabled, and
threaded backend startup calls `read_nondefault_variables()` after building the
rebound per-thread GUC table. Together those match the existing process-backend
replay path for the postmaster's serialized nondefault GUC state and move
configured built-in defaults into the early fallback session/runtime buckets
before runtime installation adopts them into `PgSession`. Remaining GUC
blockers are now focused on custom/extension behavior, database/role/startup
settings coverage, and stress validation.

### 4. Broad Threaded Startup Serialization Is Still Present

Severity: High

The threaded backend startup path is protected by a global startup mutex
because catalog/cache-heavy initialization has not been proven safe for
concurrent carriers in one address space.

Why it matters:

- a thread-per-session runtime can temporarily serialize bootstrap, but the
  gate must not mask unresolved shared-state races;
- the gate is a signal that catalog/cache/lifecycle ownership is incomplete;
- if the gate remains broad, threaded mode will have surprising scalability
  and latency behavior during connection storms;
- pooled scheduling should not start while such a coarse ownership guard is
  still needed.

Required resolution:

- document the exact state protected by the gate;
- narrow it to the smallest unsafe region or remove it;
- add a clear removal criterion;
- stress concurrent threaded startup without the gate or with only the
  narrowed gate;
- ensure normal post-bootstrap SQL execution is not accidentally serialized by
  startup safety machinery.

### 5. Static Global Classification Is Not Enforced Enough

Severity: Medium

The global lifetime scanner and annotations are useful, but they are currently
manual and heuristic. The baseline is small, which is encouraging, but the
tool does not appear to be wired into a routine validation target.

Why it matters:

- new mutable globals can be added without triggering a hard failure;
- annotations can drift from actual ownership;
- Phase 12 relies on continued state migration, so regression prevention
  matters more now than it did in early phases.

Required resolution:

- make the scanner part of the Phase 12 exit gate;
- preferably add a make/test target or documented required command for the
  agent's validation checklist;
- require explicit owner classification for new mutable globals;
- keep a full classified report available during phase reviews, not just an
  unclassified baseline.

### 6. Threaded Runtime Constraints Need To Stay Visible

Severity: Medium

Some constraints are reasonable for the experimental branch, but they must be
treated as active limitations:

- threaded backends currently require the database `LC_CTYPE` to match the
  postmaster process `LC_CTYPE`;
- Windows thread launch is not implemented;
- a failed thread carrier escalates to runtime termination;
- some worker/contrib/module coverage is represented by focused smokes rather
  than broad regression suites.

These are acceptable if documented and gated. They become a problem only if the
branch claims normal production-grade threaded behavior without resolving or
explicitly preserving these limits.

## Design Alignment

The implementation generally follows the design:

- process mode remains supported;
- thread-per-session is the first native target;
- third-party extensions are process-only by default;
- worker thread carriers are being added after regular backend carriers;
- state migration is moving from TLS toward explicit objects;
- pooled scheduling is still deferred.

The key design drift is not the use of compatibility macros. The design
expects those. The drift is that some phase completion notes read stronger
than the implementation warrants. In particular, Phase 10 and Phase 11 should
be understood as working threaded-runtime milestones, not as proof that
backend lifecycle, startup concurrency, GUC state, and worker lifecycle are
fully hardened.

## Recommended Plan Change

Add a Phase 12 exit gate before Phase 13 starts. Phase 13 introduces
scheduler-aware waits, and Phase 14 introduces pooled carriers. Both will make
ownership bugs harder to reason about. The branch should not proceed to those
phases until the remaining thread-per-session lifecycle and state ownership
issues are resolved.

The gate should require:

- safe threaded backend teardown and memory/resource cleanup;
- race-free PMChild/thread-backend lifecycle and signal routing;
- systematic threaded GUC adoption instead of a growing whitelist;
- removal or narrowing of the startup serialization gate;
- global-lifetime scanner enforcement;
- focused threaded stress for startup, shutdown, cancellation, termination,
  abandoned clients, workers, GUCs, and cleanup;
- process-mode regression remains green.

This gate belongs at the end of Phase 12, not Phase 16. Phase 16 should remain
the broader hardening phase for sanitizer runs, contrib-wide threaded
regression, platform coverage, performance baselines, and crash/FATAL matrix
work.

## Suggested Near-Term Priorities

1. Fix lifecycle cleanup before moving more state.
2. Define PMChild/thread ownership and synchronization.
3. Replace the threaded GUC whitelist with a complete adoption/rebind model.
4. Narrow or remove the threaded startup gate.
5. Promote global-lifetime scanning into a required validation step.
6. Only then continue toward scheduler-aware waits and pooled carriers.

## Progress Notes

Subsequent Phase 12 work has promoted global-lifetime scanning into
`gmake check-global-lifetimes` and moved postmaster signal/wakeup routing onto
PMChild-owned helper APIs for thread-backed backends. Thread exit publication
also now clears the logical-backend pointer and publishes exit status through a
single PMChild helper, and the exiting thread now reports retained
`TopMemoryContext` bytes to the postmaster reaper. The broad threaded startup
GUC whitelist has also been replaced for rebound built-in direct-pointer GUCs
by a systematic generated-table adoption pass. These are partial Gate E2
closures only: the full thread teardown, PMChild join/reaping contract,
postmaster/runtime default and extension/custom GUC adoption, startup-gate
narrowing, and threaded stress coverage remain blockers before Phase 13
scheduler-aware wait work.

## Bottom Line

The branch is on track only if the current debt is treated as Phase 12
correctness work. It has enough real infrastructure to become a serious
multithreaded PostgreSQL branch, but it also has enough tactical guardrails
and one-off bridges that, if left in place, would make the result a fragile
proof of concept.
