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
post-runtime required string-GUC pass has also been made ownership-based: after
`PgSetCurrentSession()` it scans built-in string GUC records and initializes
any NULL backing storage whose pointer is inside the installed `PgSession`.
Only `client_encoding` remains as a post-install compatibility exception
because its authoritative state is the session encoding object rather than a
direct `char *` field. The remaining Gate E2 GUC work is broadening
postmaster/runtime default adoption, full custom/extension GUC behavior,
broader assign-hook/reset/default semantics, and threaded stress coverage for
GUC-heavy sessions.

Further status update: threaded non-EXEC_BACKEND postmasters now write and
refresh `global/config_exec_params` when `multithreaded` is enabled, and
threaded backend startup calls `read_nondefault_variables()` after building the
rebound per-thread GUC table. Together those match the existing process-backend
replay path for the postmaster's serialized nondefault GUC state and move
configured built-in defaults into the early fallback session/runtime buckets
before runtime installation adopts them into `PgSession`. Remaining GUC
blockers are now focused on custom/extension behavior, database/role/startup
settings coverage, and stress validation.

Additional status update: custom extension GUC loading now has a first
threaded route. `dfmgr.c` tracks, per `PgSession`, which already-loaded
dynamic libraries have had `_PG_init()` invoked for that session. If a module
is reused by another threaded session, `_PG_init()` is called again so custom
GUC definitions are installed into that session's per-thread GUC table. A
required post-install string-GUC bootstrap initializes `search_path` and
`dynamic_library_path`, covering namespace lookup and `LOAD` after runtime
installation. A manual threaded `LOAD`/`SHOW` smoke proved placeholder
conversion in two sessions and the default value in a third. This is still not
the complete Gate E2 GUC model: broader custom GUC reset/default behavior,
database/role/startup settings, and stress coverage remain open.

Further status update: catalog-writing table DDL exposed a derived-GUC gap
after the custom GUC route was added. The generated
`wal_consistency_checking` string record was rebound for the session, but the
assign-hook-owned resource-manager bool array could remain NULL and crash
`XLogInsert()` during threaded `CREATE TABLE`. The required threaded session
GUC bootstrap now includes `wal_consistency_checking`, and the threaded
runtime fixture includes a basic `CREATE TABLE`/`INSERT`/`DROP TABLE` smoke.
This closes the immediate table-DDL WAL consistency pointer crash, but broader
database/role/startup settings and GUC stress remain open.

Additional status update: the threaded runtime fixture now covers a first
database/role/startup GUC matrix. It verifies a database default
(`work_mem`), role defaults (`statement_timeout` and
`default_statistics_target`), and a startup packet `options=-c lock_timeout=...`
against a threaded session. This proves the basic catalog-backed and startup
option paths for built-in GUCs; reset/default edge cases and GUC-heavy stress
remain open.

Additional status update: the threaded runtime fixture now covers the first
reset/default edge cases called out by this review. A role-backed threaded
session verifies built-in `SET LOCAL` rollback and commit behavior, `RESET`
to a database default, and `RESET` to a startup-packet `options=-c` source.
The same fixture also checks custom extension GUC `SET LOCAL` and `RESET`
semantics after per-session module initialization. The remaining GUC blockers
are broader assign-hook coverage, extension-DDL/custom-GUC stress, and larger
GUC-heavy threaded workloads.

Additional status update: the threaded runtime fixture now includes a
concurrent GUC-heavy stress block. Four simultaneous threaded sessions load
the threaded test module, repeatedly update built-in direct-pointer GUCs,
assign-hook GUCs including `wal_consistency_checking`, and per-session custom
extension GUC values, then verify transaction-local values and final session
values remain isolated. This closes the first GUC-heavy threaded workload
coverage gap; extension DDL, broader lifecycle teardown, and startup-gate
narrowing remain Gate E2 blockers.

Additional status update: concurrent temp-table abandoned-client stress found
a real threaded state-adoption crash. `PrepareTempTablespaces()` could call
`pstrdup()` on a NULL session-local `temp_tablespaces` string during threaded
`CREATE TEMP TABLE`. The required threaded GUC bootstrap now initializes
`temp_tablespaces` alongside `search_path`, `dynamic_library_path`, and
`wal_consistency_checking`. The threaded runtime fixture also adds concurrent
abandoned-client and administrator-termination stress, proving abandoned
threaded backends release advisory locks, terminated threaded backends leave
`pg_stat_activity`, and the server remains usable afterward.

Additional status update: the threaded test helper now has a real extension
packaging path. `test_backend_runtime_threaded.control` and its extension SQL
install the thread-compatible helper module as
`test_backend_runtime_threaded`, and the threaded runtime fixture now uses
`CREATE EXTENSION` instead of ad hoc C function declarations. The fixture also
checks extension-created custom-GUC helper functions and drops the extension
after all helper calls, giving Gate E2 focused thread-compatible extension DDL
coverage.

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
`TopMemoryContext` bytes to the postmaster reaper. Backend libpq teardown now
frees the frontend/backend wait set and dynamically sized send buffer in
`socket_close()`, and `Port` plus most startup packet/remote-host strings now
live in a dedicated `PortContext` that is deleted from `socket_close()`.
Follow-up work moved the connection authentication identity, forward-confirmed
remote hostname, and implicit reject HBA record into the same context.
SSL/GSS connection-owned identity state now follows the same lifetime:
`pg_gssinfo`, GSS principal strings, and SSL peer certificate names are
allocated in `PortContext`. Those connection-owned allocations therefore no
longer survive only as retained top-memory accounting.
`AuxProcessResourceOwner` is now owned by `PgBackend` behind the existing
lvalue compatibility name, with early fallback adoption for pre-runtime
initialization, so it is no longer raw backend-local TLS. `MyProc` is now also
owned by `PgBackend` behind the existing source-compatible lvalue name and
`PgCurrentMyProcRef()`, with early fallback adoption for pre-runtime
initialization. The `PGPROC` object lifecycle and shared-memory ownership are
unchanged. `MyProcNumber` and `ParallelLeaderProcNumber` now use the same
bridge through `PgCurrentMyProcNumberRef()` and
`PgCurrentParallelLeaderProcNumberRef()`, with storage inside `PgBackend` and
explicit `INVALID_PROC_NUMBER` initialization for each process or thread
backend runtime. This narrows raw backend-local TLS around proc identity state
without changing the shared-memory procarray lifecycle. `MyBEEntry` is also
now owned by `PgBackend` through `PgCurrentMyBEEntryRef()`, so the
backend-status shared-memory slot pointer follows the logical backend while
the shared `PgBackendStatus` array lifecycle remains unchanged.
`MyBgworkerEntry` now follows the same model through
`PgCurrentMyBgworkerEntryRef()`, keeping background-worker registration
identity with the logical backend while preserving the existing bgworker
registration slot and shared-memory lifecycle.
`ConfigReloadPending`, `ShutdownRequestPending`, `WakeupStopPending`,
`AutoVacLauncherPending`, and `CheckpointerShutdownXLOGPending` are now also
owned by `PgBackendPendingInterruptState` behind their existing lvalue names,
keeping generic main-loop reload/shutdown requests and the archiver,
autovac-launcher, and checkpointer-specific pending requests attached to the
logical backend.
`proc_exit_inprogress` and `shmem_exit_inprogress` are now owned by
`PgBackendExitState` behind compatibility macros in `storage/ipc.h`, so
backend exit and shared-memory-exit in-progress state follows the logical
backend exit object rather than exported standalone TLS.
`PendingBgWriterStats`, `PendingCheckpointerStats`, `pgStatBlockReadTime`,
`pgStatBlockWriteTime`, `pgStatActiveTime`, and
`pgStatTransactionIdleTime` now move as one pgstat pending state family into
`PgBackendPgStatPendingState` behind `pgstat.h` compatibility macros,
removing another set of exported backend-local TLS definitions without
changing the in-tree source-level API.
`PendingIOStats`, `have_iostats`, `pending_SLRUStats`, `have_slrustats`,
`PendingLockStats`, `have_lockstats`, `pgStatXactCommit`,
`pgStatXactRollback`, `total_func_time`, and `prevWalUsage` now follow that
same backend-owned pgstat pending bucket, further reducing raw backend-local
TLS in fixed pgstat flush/accounting paths.
`pgBufferUsage`, `save_pgBufferUsage`, `pgWalUsage`, and
`save_pgWalUsage` now move as one executor instrumentation state family into
`PgBackendInstrumentationState` behind `instrument.h` compatibility macros,
removing another fixed backend accounting group from standalone TLS.
`PendingBackendStats`, `backend_has_iostats`, `prevBackendWalUsage`,
`pgstat_report_fixed`, `pgStatForceNextFlush`,
`force_stats_snapshot_clear`, `pgstat_is_initialized`, and
`pgstat_is_shutdown` now also live in `PgBackendPgStatPendingState` behind
`pgstat.h` compatibility macros, moving backend/fixed pgstat flush state into
the same logical backend bucket. `pgStatPendingContext` and `pgStatPending`
now live in that same backend-owned pgstat bucket behind private pgstat
accessors/macros. The adoption path asserts that the early pending-entry list
is empty before moving into the logical backend, because non-empty copied
`dlist_head` values would keep node links tied to the old list head.
`pendingOps`, `pendingUnlinks`, `pendingOpsCxt`, `sync_cycle_ctr`,
`checkpoint_cycle_ctr`, `sync_in_progress`, `SMgrRelationHash`,
`unpinned_relns`, and `MdCxt` now move together into
`PgBackendStorageState`, removing another storage-owned backend-local TLS
cluster while preserving the local source names in `sync.c`, `smgr.c`, and
`md.c`. The smgr relation list has the same copied-list-head invariant as the
pgstat pending list, so early adoption asserts that no early smgr relation
hash/list exists before runtime adoption.
`VfdCache`, `SizeVfdCache`, `nfile`, `temporary_files_allowed`,
`numAllocatedDescs`, `maxAllocatedDescs`, `allocatedDescs`, and
`numExternalFDs` now also live in `PgBackendStorageState`, preserving the
local `fd.c` source names behind private compatibility macros. This exposed
one missing thread-runtime adoption edge: latch/wait setup can reserve file
descriptors before `InstallPgThreadBackendRuntimeState()`, so that install
path now adopts early storage fallback state into the thread-backed backend.
The deadlock detector workspace (`visitedProcs`, topo-sort arrays,
constraint/result arrays, `deadlockDetails`, and
`blocking_autovacuum_proc`) now lives in `PgBackendLockState` behind private
`deadlock.c` compatibility macros. The runtime object keeps the private
`deadlock.c` types opaque, so this removes the raw backend-local TLS
workspace without widening the lock-manager type surface.
Local-buffer state now lives in `PgBackendBufferState`: the exported
local-buffer arrays/counters, private `localbuf.c` hash and pin counters, and
the `GetLocalBufferStorage()` allocation cursor/context. This closes a
threading hazard that the raw global scan alone did not fully expose: the
function-local static allocation cursor would have been shared across thread
backends. The follow-up buffer-manager slice now also stores
`BackendWritebackContext`, `PinCountWaitBuf`, the shared-buffer private
refcount array/hash state, and `MaxProportionalPins` in
`PgBackendBufferState`, with object-like compatibility macros preserving the
existing buffer-manager call sites.
Backend IPC/cache-invalidation state now has a dedicated
`PgBackendIPCState`: `MyProcSignalSlot`, `SharedInvalidMessageCounter`,
`catchupInterruptPending`, and the recursive
`ReceiveSharedInvalidMessages()` buffer/cursor state now follow the logical
backend instead of remaining standalone TLS or function-local static TLS. The
slice passed clean full build/install, process-mode backend-runtime
regression, direct threaded runtime TAP, contrib build, and the required
global-lifetime scan with zero new unclassified mutable globals.
Lock-manager backend-local state now also lives in `PgBackendLockState`:
fast-path lock-group counters, relation-extension lock ownership, local lock
hash state, strong-lock progress, awaited-lock/owner state, the
deadlock-timeout pending flag, condition-variable sleep target, and
speculative insertion token state moved behind lock-manager compatibility
macros. This broadens the earlier deadlock-detector migration and removes
another coherent lock/wait state group from raw backend-local TLS. The slice
passed clean full build/install, process-mode backend-runtime regression,
direct threaded runtime TAP, contrib build, and the required global-lifetime
scan with zero new unclassified mutable globals.
Transaction/access-manager backend-local state now has a dedicated
`PgBackendTransactionState`: the transaction-status cache, two-phase
locked-GXACT and exit-registration state, private two-phase GXACT lookup
cache, SLRU error-report state, and multixact member cache/debug-string state
now follow the logical backend. This also removes two function-local statics
that were outside the raw TLS scan but still unsafe in a shared address space.
The slice passed clean full build/install, process-mode backend-runtime
regression, direct threaded runtime TAP, contrib build, and the required
global-lifetime scan with zero new unclassified mutable globals.
ProcArray visibility and XID-cache state now also lives in
`PgBackendTransactionState`: the negative `TransactionIdIsInProgress()` cache,
the per-relation-class `GlobalVisState` horizon caches, the horizon
recompute-throttle XID, and optional `XIDCACHE_DEBUG` counters now follow the
logical backend. `GlobalVisState` moved to the runtime header so the backend
state object can own it by value without changing existing snapshot/heapam
forward declarations. The slice passed clean full build/install,
process-mode backend-runtime regression, direct threaded runtime TAP, contrib
build, and the required global-lifetime scan with zero new unclassified
mutable globals.
Backend activity snapshot state now lives in a dedicated
`PgBackendActivityState`: the local backend-status snapshot table pointer,
snapshot count, and snapshot memory context now follow the logical backend.
The pgstat shared-entry reference-cache pointer, shared-reference age, and
reference-cache memory contexts now live in `PgBackendPgStatPendingState`
behind private pgstat accessors while preserving the private simplehash type
inside `pgstat_shmem.c`. `pgStatLocal` remains a dedicated follow-up because
its type depends on internal pgstat snapshot state. The slice passed clean
full build/install, process-mode backend-runtime regression, direct threaded
runtime TAP, contrib build, and the required global-lifetime scan with zero
new unclassified mutable globals.
Always-built LWLock backend-local state now also lives in
`PgBackendLockState`: the held-LWLock count, fixed held-LWLock handle array,
and backend-local user-defined tranche count now follow the logical backend
while `lwlock.c` keeps its existing local names through compatibility macros.
Optional `LWLOCK_STATS` debug-only state remains a follow-up because its dummy
stats entry uses a private debug struct and that code is not built in this
checkout. The slice passed clean full build/install, process-mode
backend-runtime regression, direct threaded runtime TAP, contrib build, and
the required global-lifetime scan with zero new unclassified mutable globals.
Predicate-lock backend-local state now also lives in `PgBackendLockState`:
the local predicate-lock hash table, current serializable transaction pointer,
write-tracking flag, and saved serializable transaction pointer now follow the
logical backend while `predicate.c` keeps local compatibility macros. Private
`SERIALIZABLEXACT` layout stays private to predicate locking through opaque
runtime pointers. The slice passed touched-object builds, backend
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
Backend utility/support state now lives in `PgBackendUtilityState`: dynahash
active sequential-scan tracking, the superuser one-entry cache, the
resource-owner release callback list pointer, and optional `RESOWNER_STATS`
lookup counters now follow the logical backend. `ResourceReleaseCallbackItem`
remains private to `resowner.c` through an opaque runtime pointer and
file-local typed helper. The slice passed clean full build/install,
process-mode backend-runtime regression, direct threaded runtime TAP, contrib
build, and the required global-lifetime scan with zero new unclassified
mutable globals; backend-local declarations dropped from 288 to 280.
Utility cache/scratch state now also lives in `PgBackendUtilityState`:
date/time token caches, degree-trig cached constants, date/time and numeric
format-picture caches, the optional libxml allocation context, and the
missing-attribute datum cache follow the logical backend. Private cache entry
types stay private to `datetime.c` and `formatting.c` through opaque runtime
pointer arrays and file-local casts. The slice passed clean full
build/install, process-mode backend-runtime regression, direct threaded
runtime TAP, contrib build, and the required global-lifetime scan with zero
new unclassified mutable globals; backend-local declarations dropped from 280
to 262.
Parallel worker and pqmq backend-local state now lives in
`PgBackendParallelState`: exported worker number/message/initialization flags,
private parallel context tracking, and shared-memory message queue redirection
state follow the logical backend. Private `FixedParallelState` and
`shm_mq_handle` types stay local to their owning files through opaque runtime
pointers. The slice passed clean full build/install, process-mode
backend-runtime regression, direct threaded runtime TAP, contrib build, and
the required global-lifetime scan with zero new unclassified mutable globals;
backend-local declarations dropped from 262 to 249.
DSM initialization, DSM registry, local latch, and latch wait-set state now
also live in `PgBackendIPCState`. The slice exposed an important startup
ordering invariant: threaded backend startup calls `InitProcessLocalLatch()`
and `InitializeLatchWaitSet()` before installing the backend runtime object, so
runtime adoption must retarget adopted early `backend->core.latch` and
`backend->interrupt_latch` pointers to the backend-owned latch before clearing
the early fallback. The slice passed clean full build/install, process-mode
backend-runtime regression, direct threaded runtime TAP, contrib build, and
the required global-lifetime scan with zero new unclassified mutable globals;
backend-local declarations dropped from 249 to 244.
Timeout scheduler state now lives in `PgBackendTimeoutState`: registered
timeout parameters, the active timeout queue, alarm/signal pending flags,
firing-target pointers, and signal-vs-logical delivery mode follow the logical
backend. `timeout.c` keeps its existing scheduling behavior through
compatibility macros over the current backend timeout bucket. The slice passed
clean full build/install, process-mode backend-runtime regression, direct
threaded runtime TAP, contrib build, and the required global-lifetime scan with
zero new unclassified mutable globals; backend-local declarations dropped from
244 to 236.
WAL sender state now lives in `PgBackendWalSenderState`: exported WAL sender
identity and wakeup flags, streaming cursor/timeline state, reply/keepalive
timestamps, shutdown flags, replication command scratch buffers, uploaded
manifest state, logical decoding context, replication command memory context,
and lag tracker now follow the logical backend. Public headers retain the old
names as compatibility macros over `PgCurrentWalSenderState()`, and
`walsender.c` uses a distinct `local_sent_ptr` macro to avoid colliding with
the shared-memory `WalSnd.sentPtr` field. The slice passed clean full
build/install, process-mode backend-runtime regression, direct threaded
runtime TAP, contrib build, and the required global-lifetime scan with zero
new unclassified mutable globals; backend-local declarations dropped from 236
to 202.
Replication receiver and slot state now lives in
`PgBackendReplicationState`: `MyReplicationSlot`, synchronous replication wait
mode, and WAL receiver connection/file/logstream/wakeup/reply state now follow
the logical backend. Public slot references retain the `MyReplicationSlot`
compatibility name over `PgCurrentReplicationState()`, while `syncrep.c` and
`walreceiver.c` keep source-local compatibility names. The runtime initializer
preserves the old non-zero sentinels for sync-rep no-wait, receive-file `-1`,
and primary-standby-xmin true. The slice passed clean full build/install,
process-mode backend-runtime regression, direct threaded runtime TAP, contrib
build, and the required global-lifetime scan with zero new unclassified
mutable globals; backend-local declarations dropped from 202 to 193.
Logical replication worker state now lives in
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
PMChild cleanup and slot release now require a
successful native thread join; a join failure restores the claimed thread-exit
report and leaves the PMChild active for retry instead of releasing a possibly
still-owned slot. PMChild thread-exit publication now
captures the exited logical backend id in the exit payload and clears live
`signal_pid` under the same lock as `thread_backend`, while PMChild assignment
and slot release scrub stale carrier-visible signal ids and thread-exit
payloads before reuse. A later hardening pass moved the SetProcess,
SetThread, and ReleasePostmasterChildSlot thread-payload scrubs under the same
PMChild mutex, so slot reuse cannot race with thread-backed signal-id,
interrupt, wakeup, or exit-payload readers. Thread-backed signal-id reads and
claimed thread-exit payload reads now also use PMChild helper APIs under the
same PMChild mutex.
Thread exit now has an explicit PMChild detach boundary before final exit
publication, so the live `thread_backend` pointer is cleared before the carrier
continues through final teardown accounting. The focused backend-runtime
regression now includes a native-thread race helper that hammers PMChild
signal-id, interrupt, and wakeup reads while the owner repeatedly publishes a
backend pointer, detaches it, publishes exit, and claims the exit payload.
Threaded regular backend socket handoff is now explicit: the launch-time
`ClientSocket` copy remains valid until `pq_init()` has copied the descriptor
into `Port` and registered `socket_close()`, and `backend_thread_finish()`
closes the copied descriptor if startup exits before that ownership transfer.
The broad threaded startup GUC
whitelist has also been replaced for rebound built-in direct-pointer GUCs by
a systematic generated-table adoption pass, and threaded built-in postmaster
default replay now uses the existing serialized nondefault GUC file path.
The threaded runtime fixture now also includes a test-extension helper that
raises backend-local `FATAL`, captures the SQL-visible logical backend id,
verifies the backend leaves `pg_stat_activity`, and confirms the server
remains usable afterward.
Making the local TAP dependency available then exposed and fixed a threaded
SIGHUP/default-replay bug: dynamic-default `client_encoding` was being
serialized from stale generic string storage, so a late thread-backed IO worker
could replay garbage and terminate the threaded server. `client_encoding` is
now the only post-install required-string compatibility exception, while other
session-owned string GUCs are initialized by an ownership scan, and
`client_encoding` is serialized from authoritative encoding state.
Real-server teardown coverage has also been broadened: the threaded runtime
TAP now runs concurrent backend-local `FATAL`, administrator termination, and
abandoned-client exits in one live threaded server, then verifies logical
backend ids and advisory locks are gone and the server remains usable.
The focused `test_backend_runtime` regression is also usable again as a
process-mode validation control after fake thread-runtime tests were changed
to construct thread-backend state without installing it into the active SQL
backend.
Thread-backed auxiliary loops that use the logical interrupt mailbox now
honor `ProcDiePending`, fixing the basic immediate-shutdown smoke for
background writer, checkpointer, autovacuum launcher, and WAL writer thread
carriers. The temporary threaded startup serialization gate was narrowed to no
remaining backend-type users and then removed instead of being retained as a
no-op helper. Regular client backend startup can run without serialization
after moving the recursive
VACUUM/ANALYZE guard from a function-local static into
`PgExecutionVacuumState`; a 32-connection threaded
startup/catalog/temp-table/ANALYZE stress validated the no-gate path. The
writer-class, startup process, autovacuum launcher/workers, thread-compatible
background workers, archiver, WAL receiver, WAL summarizer, and slot sync
worker bypasses are worker-specific narrowings with concrete startup ownership
models. Process-model background workers remain rejected in threaded mode.
Thread-compatible dynamic background workers now
publish postmaster-visible startup only after
`ThreadedBackendStartupComplete()`, preventing dynamic waiters from
terminating a worker while `InitProcess()`, `BaseInit()`, or function lookup
are still running. The autovacuum launcher narrowing is validated against the
no-database launcher loop, while autovacuum worker narrowing is validated
against a real database-connected autovacuum worker launch and table vacuum
smoke. Startup process was additionally validated through threaded normal
startup and crash recovery, while archiver, WAL receiver, and WAL
summarizer were additionally validated through their wakeup/progress,
streaming, and clean shutdown paths. Slot sync worker was additionally
validated through a threaded physical standby smoke that synchronized a
failover logical slot from the primary and verified standby catalog usability.
A broader attempted bypass for other
non-session auxiliary workers reproduced an abrupt postmaster death during a
threaded `pg_class` catalog scan, so future startup-gate reintroduction still
requires a named shared-state dependency and catalog-startup stress coverage.
These were partial Gate E2 closures at the time of the first review. Later
work added real-server PMChild termination/reaping stress coverage, moved the
stale runtime-global rendezvous hash and reserved GUC prefix storage under
`PgRuntime.extension_modules`, and enabled deletion of the exiting carrier's
`TopMemoryContext` after closed backend/session/connection/execution cleanup.
The direct threaded runtime TAP now passes with that reclamation path enabled.
Representative threaded contrib coverage now installs and exercises `hstore`,
`pg_trgm`, `btree_gist`, and `pageinspect` in the threaded TAP. Those modules
now carry thread-per-session backend-model metadata, with `pg_trgm`'s custom
GUC backing variables moved to session-local TLS storage before opt-in. Phase
16 still owns contrib-wide threaded regression and modules that need a broader
state/export audit before thread opt-in.
A follow-up object-model review keeps the current direction but raises one
additional Gate E2 hardening requirement: `PgBackend`, `PgSession`,
`PgConnection`, `PgExecution`, and `PgThreadBackendRuntimeState` now form a
coherent object shape, but manual initializer/adoption lists and pointer/list-
bearing bucket copies are now a real correctness risk. Before Phase 12 closes,
each state bucket needs an explicit lifecycle classification covering
initializer, early-adoption behavior or proof that early adoption is
impossible, reset/destroy behavior, owner/lifetime, and copy/adoption rules
for pointers, list heads, memory contexts, sockets, hash tables, and opaque
pointers. Process/runtime initialization and thread-runtime installation must
be centralized or have every intentional asymmetry documented. The same review
also requires documenting the endpoint for the `PgSession`/legacy `Session`
bridge and treating `PgBackend` as a Phase 12 consolidation bridge, not the
final ownership boundary.
Subsequent Gate E2 hardening addressed the first adoption-asymmetry concern by
adding `PgBackendAdoptEarlyState()` and making both process runtime
initialization and thread backend installation call it. That brings the
previously process-only WAL sender, replication, logical replication, XLog,
recovery, maintenance-worker, autovacuum, repack, AIO, pending-interrupt, and
interrupt-holdoff adoption paths into thread install. The same slice fixed one
pointer/list-bearing bucket rule by asserting an empty early autovacuum
database list and reinitializing the adopted backend list head instead of
copying a fallback `dlist_head` self-pointer. This is a partial Gate E2
closure only; the full bucket lifecycle audit, session/execution completion,
legacy `Session` endpoint, and destructor/reset model remain blockers.
Further hardening centralized the session and execution counterparts in
`PgSessionAdoptEarlyState()` and `PgExecutionAdoptEarlyState()`, so process
runtime initialization and thread backend installation no longer maintain
parallel manual lists for those object families either. Focused regression
coverage now verifies representative session/execution fallback adoption and
reset. Further hardening added the connection counterpart,
`PgConnectionAdoptEarlyState()`, with a preserved-port rule for threaded
backend installation. That closes the immediate process/thread adoption-list
asymmetry across backend, session, connection, and execution objects. The
broader connection-lifetime audit remains open because connection state owns
or borrows pointer-bearing resources such as send buffers, wait sets, security
buffers, and authentication strings. A later Gate E2 teardown slice added
`PgConnectionResetClosedState()`: `socket_close()` releases the palloc-backed
send buffer and frontend/backend wait set, then the runtime helper scrubs the
retained connection socket/protocol/startup/security buckets and frees the
malloc-backed GSS buffers. This closes one concrete connection reset/destroy
rule; the complete destructor tree and `TopMemoryContext` ownership model
remain blockers.
Another teardown slice added `PgSessionResetClosedState()` for the
per-session dynamic-library `_PG_init()` replay list. `dfmgr.c` now allocates
the `dynamic_library_inits` list cells under a session-owned
`dynamic_library_context`, and backend exit deletes that context only after
`on_proc_exit` callbacks run. This closes one concrete list-bearing
`PgSession` reset/destroy rule. Follow-up bridge hardening moved the legacy
`access/session.h` payload allocation behind `PgSessionGetLegacySession()`,
added `PgSession.legacy_session_context` to the checked lifecycle manifest,
and deletes that context during `PgSessionResetClosedState()` after DSM/DSA
detach paths have run. A matching execution cleanup slice clears retained
`PgExecution.memory_contexts` slots at the end of backend-exit cleanup, after
session/backend reset has finished using live memory-context state. This
closes the previously pending lifecycle manifest rows. Further session-cache
teardown now drops prepared statements, destroys the prepared-query hash,
frees leftover `ON COMMIT` list cells, and destroys any remaining async
local-channel hash after proc-exit async callbacks have run. The later
carrier-root reclamation probe now deletes the retained `TopMemoryContext`
after closed-state cleanup; remaining memory findings should name a concrete
runtime owner or teardown bug instead of relying on retained-root accounting.
The next state-migration batch added `PgExecutionCatalogState` and moved seven
catalog execution globals into it: uncommitted enum hash pointers, REINDEX
suppression state, and pending smgr delete/sync state. Existing enum, reindex,
and smgr transaction cleanup remains authoritative for the pointed-to
hash/list storage; the runtime object now owns the carrier-independent pointer
slots. The global-lifetime scan now reports 88 execution-local declarations,
down from 95, with zero new unclassified mutable globals.
The following state-migration batch added `PgExecutionAsyncState` and moved
LISTEN/NOTIFY transaction scratch state into it: pending LISTEN/UNLISTEN
actions, pending NOTIFY lists, pending listen intent hash state, queue head
snapshots, and `SignalBackends()` workspace arrays. Existing async
transaction cleanup and transaction memory contexts remain authoritative for
the pointed-to list/hash storage. The global-lifetime scan now reports 81
execution-local declarations, down from 88, with zero new unclassified mutable
globals.
Further hardening made the object-lifecycle audit mechanically enforceable:
`MULTITHREADED_RUNTIME_LIFECYCLE.tsv` now records owner/lifetime, initializer,
early-adoption, reset/destroy, and copy/adoption rules for every current
`PgBackend`, `PgSession`, `PgConnection`, and `PgExecution` field, and
`gmake check-runtime-lifecycles` fails if the manifest misses a field or
contains a stale field. This does not close the lifecycle blocker by itself;
it turns the bucket audit into a required validation target and makes the
remaining reset/destroy rows mechanically visible. Subsequent bridge cleanup
closed the pending legacy-session and execution-memory-context rows, so new
Gate E2 lifecycle debt should show up either as a manifest check failure or as
an explicitly added pending row.
Further Gate E2 hardening removed the remaining duplicated process-mode
constructor path for runtime objects. `InitializePgProcessRuntime()` now uses
the same backend/session/connection/execution object constructors as
`InitializePgThreadBackendRuntimeState()`, while process and thread install
paths both go through the top-level early-adoption helpers. The lifecycle
checker now also verifies manifest-referenced runtime lifecycle function names
against the checked runtime sources and asserts these constructor/adoption
calls remain present. This addresses the concrete manual-list asymmetry risk;
it does not change the longer-term assessment that `PgBackend` is a Phase 12
state-consolidation bridge rather than the final per-subsystem ownership
model.
The latest state-migration slice moved wait-event storage into
`PgBackendWaitState` and the shared-invalidation local transaction ID counter
into `PgBackendIPCState`. Validation included touched-object builds, a clean
full build, install, `gmake check-global-lifetimes`, contrib build, PL/pgSQL
rebuild/install, `test_backend_runtime` regression, and direct threaded TAP.
The global-lifetime scan now reports 47 backend-local declarations with zero
new unclassified mutable globals. This slice also exposed a stale
`src/common` server-object hazard after removing the exported
`my_wait_event_info` symbol; clean `src/common` when runtime/header changes
affect headers included by `src/common` server objects.
The next state-migration slice moved `DoingCommandRead` into
`PgSessionLoopState`, moved tcop's `-D` option and usage snapshots into
`PgBackendCommandState`, and moved elog's formatted start-time buffer, log
line counter, and cached log PID into `PgBackendLogState`. Validation included
touched-object builds, a backend plus `src/common` clean rebuild, clean full
build, install, `gmake check-global-lifetimes`, contrib build, PL/pgSQL
rebuild/install, `test_backend_runtime` regression, and direct threaded TAP.
The global-lifetime scan now reports 44 backend-local declarations with zero
new unclassified mutable globals.
The next state-migration slice moved the cumulative statistics
`pgStatLocal` anchor into `PgBackendPgStatPendingState`, leaving the existing
identifier as a compatibility macro over `PgCurrentPgStatLocalState()`.
Validation included touched-object builds, a backend plus `src/common` clean
rebuild, clean full build, install, `gmake check-global-lifetimes`, contrib
build, PL/pgSQL rebuild/install, `test_backend_runtime` regression, and direct
threaded TAP. The global-lifetime scan now reports 42 backend-local
declarations with zero new unclassified mutable globals. This slice adds a
temporary `backend_runtime.h` to `pgstat_internal.h` include edge so the
pgstat local object can stay embedded; the Gate E2 header-boundary audit
should revisit that coupling.
The next state-migration slice moved the computed-goto expression interpreter
dispatch and reverse-lookup tables into `PgBackendExprInterpState`. Validation
included touched-object builds, a backend plus `src/common` clean rebuild,
clean full build, install, `gmake check-global-lifetimes`, contrib build,
PL/pgSQL rebuild/install, `test_backend_runtime` regression, and direct
threaded TAP. The global-lifetime scan now reports 41 backend-local
declarations with zero new unclassified mutable globals. The runtime bucket
uses a fixed `PG_BACKEND_EXPR_INTERP_MAX_OPS` capacity with an
`execExprInterp.c` assertion rather than adding an executor include edge to
`backend_runtime.h`.
The next state-migration slice completed the optional LWLock debug-statistics
bridge by moving `lwlock_stats_htab`, the dummy stats entry, the stats memory
context pointer, and the exit-callback registration flag into
`PgBackendLockState`. This also removes the hidden function-local statics in
`init_lwlock_stats()`, so the optional debug path follows the logical backend
rather than the shared address space. Validation included touched-object
builds, a backend plus `src/common` clean rebuild, clean full build, install,
`gmake check-global-lifetimes`, contrib build, PL/pgSQL rebuild/install,
`test_backend_runtime` regression, and direct threaded TAP. The
global-lifetime scan now reports 39 backend-local declarations with zero new
unclassified mutable globals. The checkout's normal build does not compile
the `LWLOCK_STATS` block itself, so direct compile coverage for that debug
path still requires an `LWLOCK_STATS`-enabled build.
The next state-migration slice moved snapshot-manager and combo-CID
transaction visibility state into `PgExecution`: current/secondary/catalog/
historic snapshot pointers and reusable `SnapshotData`, active and registered
snapshot tracking, `TransactionXmin`, `RecentXmin`, `FirstSnapshotSet`,
exported-snapshot tracking, historic tuple-CID state, and combo-CID hash/array
state now follow the logical execution. `snapmgr.c` keeps its active-stack
type and registered-snapshot comparator private, lazily initializing the
runtime-owned heap. Validation included touched-object builds, a backend plus
`src/common` clean rebuild, clean full build, install,
`gmake check-global-lifetimes`, contrib build, PL/pgSQL rebuild/install,
`test_backend_runtime` regression, and direct threaded TAP. The
global-lifetime scan now reports 134 execution-local declarations with zero
new unclassified mutable globals, down from 154 before this slice.
The next execution-state slice moved WAL record-construction workspace from
`xloginsert.c` into `PgExecution`: registered-buffer workspace, main-data
`XLogRecData` chain state, current insert flags, header record/scratch
storage, registered-data array state, in-progress flag, and the workspace
memory context now follow the logical execution. `registered_buffer` remains
private to `xloginsert.c` behind an opaque runtime pointer. The adoption path
asserts that no WAL insert is in progress and retargets the legacy
`mainrdata_last` self-pointer sentinel from the early fallback bucket to the
destination execution bucket. Validation included touched-object builds, a
backend plus `src/common` clean rebuild, clean full build, install,
`gmake check-global-lifetimes`, contrib build, PL/pgSQL rebuild/install,
`test_backend_runtime` regression, and direct threaded TAP. The
global-lifetime scan now reports 121 execution-local declarations with zero
new unclassified mutable globals, down from 134 before this slice. The
function-local fake-LSN statics in `XLogGetFakeLSN()` remain a documented
follow-up needing a separate session/execution lifetime decision.
The next transaction-state slice moved the simple exported transaction
execution flags into `PgExecution`: `XactIsoLevel`, `XactReadOnly`,
`XactDeferrable`, `xact_is_sampled`, `CheckXidAlive`, `bsysscan`, and
`MyXactFlags` now follow the logical execution behind `xact.h` lvalue
compatibility macros. The runtime implementation keeps the storage in
`backend_runtime.c`, while `xact.h` only declares accessors so it does not
need to include `backend_runtime.h`. Validation included touched-object
builds, a clean backend plus `src/common` rebuild, full `gmake -j8`, install,
contrib build, clean PL/pgSQL rebuild/install, `gmake check-global-lifetimes`,
the test-backend-runtime regression, and the direct threaded runtime TAP. The
global-lifetime scan now reports 108 execution-local declarations with zero
new unclassified mutable globals, down from 121 before this slice. The
private transaction-state stack, command-id state, timestamps, callback lists,
and transaction abort context remain a documented follow-up needing a broader
lifecycle split.
The next GUC/error scratch-state slice moved GUC check-hook error
code/message/detail/hint state, `pre_format_elog_string()` errno/domain
scratch state, and config-file scanner line/fatal-jump scratch state into
`PgExecution`. The public GUC check-hook string names remain source-compatible
lvalue macros in `guc.h`, while `guc.c`, `elog.c`, and `guc-file.l` keep
private names through file-local compatibility macros. Validation included
touched-object builds, stale-symbol link/load failures that confirmed the
installed-header clean-rebuild requirement, clean backend plus `src/common`
rebuild, full `gmake -j8`, install, clean PL/pgSQL rebuild/install, the
test-backend-runtime regression, contrib build, `gmake check-global-lifetimes`,
and direct threaded runtime TAP. The global-lifetime scan now reports 97
execution-local declarations with zero new unclassified mutable globals, down
from 108 before this slice.
The next miscellaneous execution scratch-state slice moved array typanalyze
callback scratch, regex locale scratch, the optional Valgrind command-loop
error counter, and logical-decoding snapshot-builder exported-snapshot scratch
state into `PgExecution`. The pointer-bearing fields are explicitly classified
as borrowed or opaque execution-scope pointers, with no owned lists, memory
contexts, hash tables, sockets, or heap allocations. Process runtime
initialization and thread runtime installation both adopt the early fallback
buckets, avoiding a new manual adoption asymmetry for this slice. Validation
included touched-object builds, `gmake check-global-lifetimes`, clean backend
plus `src/common` rebuild, full `gmake -j8`, install, clean PL/pgSQL
rebuild/install, contrib build, the test-backend-runtime regression, and
direct threaded runtime TAP. The global-lifetime scan now reports 95
execution-local declarations with zero new unclassified mutable globals, down
from 97 before this slice.
The next transaction-state slice moved a larger scalar/pointer group from
`xact.c` into `PgExecutionXactState`: top full transaction ID,
parallel-current-XID count and borrowed pointer, inline unreported-XID array,
subtransaction and command ID counters, transaction timestamps, prepare GID,
force-sync flag, and transaction abort context pointer. The serialized
parallel-transaction state fields were renamed locally so `xact.c` compatibility
macros do not rewrite `tstate->field` references. The runtime lifecycle
manifest now documents the inline array rule and borrowed-pointer ownership,
while the private transaction-state stack and callback lists remain explicit
follow-up work. Validation included touched-object builds, full `gmake -j8`,
the `test_backend_runtime` regression, direct threaded TAP after reinstalling
the temp install, `gmake check-runtime-lifecycles`, `gmake
check-global-lifetimes`, `gmake -C contrib -j8`, and `git diff --check`. The
global-lifetime scan now reports 67 execution-local declarations with zero new
unclassified mutable globals, down from 81 before this slice.

The following transaction-cleanup slice moved large-object cleanup slots,
transaction temporary-file cleanup state, the pgstat subtransaction stack
pointer, and RI fast-path batch-cache state into
`PgExecutionTransactionCleanupState`. The moved pointers are explicitly
borrowed: large-object, temporary-file, pgstat, and RI cleanup remain owned by
their existing transaction/subtransaction cleanup paths, while `PgExecution`
owns the slots and scalar flags. The lifecycle manifest now captures that
copy/adoption rule, and both process runtime initialization and thread runtime
installation adopt the bucket through `PgExecutionAdoptEarlyState()`.
Validation included touched-object builds, full `gmake -j8`, the
`test_backend_runtime` regression, direct threaded TAP, `gmake
check-runtime-lifecycles`, `gmake check-global-lifetimes`, `gmake -C contrib
-j8`, and `git diff --check`. The global-lifetime scan now reports 60
execution-local declarations with zero new unclassified mutable globals, down
from 67 before this slice.

The following execution-scratch slice moved `elog.c`'s error-data stack,
recursion depth, saved timestamp cache, and formatted log-time buffer into
`PgExecutionErrorState`, and event-trigger query state, replication-origin
transaction state, logical apply error-context stack, logical apply message
context, and logical streaming context into
`PgExecutionReplicationScratchState`. The pointer-bearing fields are recorded
as borrowed slots whose storage remains owned by existing error,
event-trigger, and logical-apply cleanup paths; replication-origin transaction
state is copied scalar state. Validation included touched-object builds, full
`gmake -j8`, install, `test_backend_runtime` regression, direct threaded TAP,
`gmake check-runtime-lifecycles`, the required global-lifetime scan, contrib
build, and `git diff --check`. The global-lifetime scan now reports 47
execution-local declarations with zero new unclassified mutable globals, down
from 60 before this slice.

The following catalog-cache slice moved catcache's create-in-progress stack
and relcache's build-in-progress list, EOXact relation OID list, and EOXact
tupledesc array slots into `PgExecutionCatalogCacheState`. The lifecycle
manifest records the inline OID list as object-owned scalar storage and the
catcache/relcache pointer slots as borrowed from existing stack,
`CacheMemoryContext`, and relcache EOXact cleanup ownership. Validation
included touched-object builds, full `gmake -j8`, `test_backend_runtime`
regression, `gmake check-runtime-lifecycles`, the required global-lifetime
scan, contrib build, and direct threaded runtime TAP after forcing the
runtime-layout rebuild path for stale `launch_backend.o`. The global-lifetime
scan now reports 38 execution-local declarations with zero new unclassified
mutable globals, down from 47 before this slice.

The following session-cache slice moved the text-search parser, dictionary,
and configuration cache hashes plus their last-used entry pointers into
`PgSessionTextSearchState`. This closes a representative pointer/hash-bearing
session-cache bucket rather than only scalar GUC state: closed-session reset
now destroys parser/config hash tables, dictionary private memory contexts,
config map arrays, and last-used pointers explicitly. Validation included
touched-object builds, `gmake check-runtime-lifecycles`, and the required
global-lifetime scan. The global-lifetime scan now reports 191 session-local
declarations and 30 execution-local declarations with zero new unclassified
mutable globals.

The following user-identity cache slice moved `acl.c`'s role-membership cache
arrays and cached database hash into `PgSessionUserIdentityState`. The cache
lists are copied into `TopMemoryContext` by the existing ACL code, but are now
session object slots with explicit invalidation and `PgSessionResetClosedState()`
cleanup. Validation included touched-object builds, `gmake
check-runtime-lifecycles`, and the required global-lifetime scan. The
global-lifetime scan now reports 188 session-local declarations and 30
execution-local declarations with zero new unclassified mutable globals.

The following function-manager cache slice moved `fmgr.c`'s external C
function lookup hash behind `PgSessionFunctionManagerState`. The hash entries
remain borrowed metadata references into fmgr/dynamic-loader state; the
session reset path owns hash destruction, not dynamic library handles. The
global-lifetime scan count remains 188 session-local declarations because the
standalone TLS hash was replaced by the early fallback session bucket, while
`gmake check-runtime-lifecycles` now checks 136 runtime fields.

The following invalidation-callback slice moved `inval.c`'s syscache,
relcache, and relsync callback registries behind
`PgSessionInvalidationCallbackState`. Callback entries are function pointers
plus `Datum` arguments; the target cache storage remains owned by the
registering subsystem. `PgSessionResetClosedState()` clears the registry after
dependent session caches are destroyed, so callback registrations do not leak
across logical session close/reuse. The runtime lifecycle checker now covers
137 object fields.

The same slice hardened PMChild/thread-backend synchronization by moving the
thread-start postmaster latch capture to immediately after thread runtime-state
initialization, adding a fallback to the current local latch data, and
asserting the payload latch is non-NULL before creating the carrier. This
prevents threaded startup completion and exit publication from calling
`SetLatch(NULL)` if `MyLatch` is not yet visible through the runtime wrapper.

The following datetime slice moved `datetime.c`'s active
`TimeZoneAbbrevTable` pointer and recent timezone-abbreviation lookup cache
behind `PgSessionDateTimeState`. This is a narrow session-cache migration with
an explicit ownership rule: the table pointer is borrowed from GUC extra
storage, and the inline cache is session scratch reset by
`InstallTimeZoneAbbrevs()` and `ClearTimeZoneAbbrevCache()`. The
session-local runtime test now switches fake sessions and verifies both the
borrowed pointer slot and cache entry remain isolated.

The next logical-replication session-cache slice moved replication-origin's
borrowed session slot, subscriber relation-map and partition-map roots,
`pgoutput` relation sync state, and sync-worker relation validity into
`PgSessionLogicalReplicationState`. This bucket is still a bridge, but it has
an explicit lifecycle rule: early adoption asserts all pointer/hash/context
slots are empty, closed-session reset deletes owned relation-map contexts and
hashes, and replication-origin refcount release remains with
`replorigin_session_reset()`/exit cleanup instead of being hidden inside the
generic session destructor.

The following catalog-lookup session-cache slice moved attribute-options,
relfilenumber, tablespace-options, event-trigger, ruleutils SPI-plan, and ICU
converter roots into `PgSessionCatalogLookupState`. This addresses the review
concern about pointer/hash/context-bearing buckets by giving the whole batch a
single lifecycle row and fake-session isolation test. The row deliberately
calls out the remaining limitation: hash roots, SPI plans, event-trigger
contexts, and ICU converters have reset paths now, but pointed allocations
under `CacheMemoryContext` are still owned by the unresolved cache memory
context split.

The following in-tree extension session-state slice added
`PgSessionExtensionModuleState`, an opaque per-session private-state pointer
plus reset callback list, and moved PL/pgSQL's remaining session-local
custom-GUC, compile, namespace, plugin, simple-expression, and cast-cache
state behind it. PL/pgSQL now registers a closed-session reset callback that
frees cached cast expressions and leftover simple-expression roots before
`dynamic_library_context` is deleted. This establishes the intended route for
bundled extension-owned session state without exposing PL/pgSQL internals in
`backend_runtime.h`; contrib-wide opt-in and regression remain Phase 16 work.

The next tcop session-state slice added `PgSessionTcopState` and moved
`postgres.c`'s unnamed prepared statement pointer, interactive `-E`/`-j`
switches, and reused row-description context/buffer into it. The switch
accessors retain early fallback storage for single-user option parsing before
a session object exists; adoption asserts the pointer-bearing plan/context
slots are still empty. Closed-session reset now drops any leftover unnamed
cached plan and deletes the row-description context explicitly.

The following session utility-state slice added `PgSessionXactCallbackState`
and `PgSessionBackupState`, moving `xact.c` callback list heads, SQL backup
payload pointers, backup context, and WAL session backup status behind
`PgSession`. This slice includes explicit teardown: closed-session reset frees
leftover callback list nodes, aborts a still-running SQL backup through the
existing WAL cleanup path, deletes the backup memory context, and clears the
payload/status slots.

The following session cache and flag slice added `PgSessionRIGlobalsState`
and `PgSessionRelMapState`, moving RI trigger cache roots, the RI valid-entry
list, `debug_discard_caches`, loaded relation-map files, and
`update_process_title` into `PgSession`. The active/pending relation-map
transaction update files remain execution-owned. This slice deliberately
records the remaining RI saved-SPI-plan memory limitation in the lifecycle
manifest while still removing eight raw session-local globals from independent
TLS storage. Validation included touched-object builds, clean full build,
install, contrib build, backend-runtime regression, direct threaded runtime
TAP, `gmake check-runtime-lifecycles`, `gmake check-global-lifetimes`, and
`git diff --check`.

The following central GUC-registry slice added `PgSessionGUCState`, moving
`GUCMemoryContext`, copied GUC records, the GUC hash table,
non-default/stack/report list heads, reporting state, and `GUCNestLevel` into
`PgSession`. Threaded and test fake sessions now build their own per-session
GUC registry instead of relying on a shared process-global hash table. The
owner bucket is adopted before GUC-backed string buckets, so early fallback
strings copied into datetime, text-search, and connection state remain owned by
the destination session's GUC context. This slice also hardened copy/adopt
rules by retargeting moved dlist/dclist heads, including the GUC non-default
list and RI valid-entry dclist. Detached early string buckets are left
uninitialized and NULL after owner transfer, so startup-thread cleanup neither
allocates fresh fallback GUC-owned strings nor frees non-owned fallback
defaults while runtime installation is only partially complete. Validation
exposed a separate retained-memory teardown issue:
threaded backend cleanup can see AllocSet freelist entries that belong to the
retained `TopMemoryContext` accounting path. Thread-mode memory-manager reset
now clears that bucket instead of freeing retained context headers; process
mode keeps the destructive freelist cleanup. The batch also adds a temporary
process-wide GUC critical section around threaded session GUC setup, mutation,
and display while copied GUC metadata, check hooks, assign hooks, and show
hooks still carry process-era assumptions; it should be narrowed once the
remaining GUC-backed globals are session-owned. Threaded nondefault replay now
skips `PGC_POSTMASTER` and `PGC_INTERNAL` records so thread carriers do not
replace/free process-global strings already inherited from the postmaster
address space. It also tested a broader startup serialization gate, but an
unconditional `backend_thread_entry()` gate was rejected because it can block
normal threaded startup behind worker paths that have not reached
`ThreadedBackendStartupComplete()`. The remaining no-op startup-gate helper was
removed rather than retained; any future startup serialization must name the
shared-state dependency, use a narrow critical section, and include a
release/stress test. Early fallback state, GUC replay, runtime installation,
backend initialization, and worker initialization remain explicit Gate E2 audit
targets rather than being hidden behind a process-wide startup lock. Follow-up
validation made `CurrentPgRuntime` a carrier/thread-local current
binding, matching the other current runtime objects, and moved
`reserved_class_prefix` allocation out of session `GUCMemoryContext` storage
into runtime-owned extension-module storage. This closes the PL/pgSQL after
backend `FATAL` crash where runtime-global prefix
metadata pointed into a destroyed backend/session context.
Validation included clean full build, backend-runtime regression,
`gmake check-runtime-lifecycles`, `gmake check-global-lifetimes`, and
`git diff --check`. Subsequent Gate E2 session-cache batches moved portal
manager roots, compiled-regexp cache roots, syscache root arrays, the catcache
header, relcache root hashes/flags/counters, and typcache root
hashes/stacks/counters behind `PgSession`. The global-lifetime scan now
reports 112 session-local declarations with zero new unclassified mutable
globals. Remaining cache-state blockers include `CacheMemoryContext`,
`funccache.c`, and JIT/provider caches, each of which needs an explicit
lifecycle rule before Phase 12 closes.

## Bottom Line

The branch is on track only if the current debt is treated as Phase 12
correctness work. It has enough real infrastructure to become a serious
multithreaded PostgreSQL branch, but it also has enough tactical guardrails
and one-off bridges that, if left in place, would make the result a fragile
proof of concept.

## Phase 12 Organization Steering

The Gate E2 lifecycle discipline should stay manifest-checked, but
`backend_runtime.c` should not become the permanent owner for every accessor
and lifecycle helper. The branch now has two adjacent fork-owned runtime bridge
files proving the intended direction:

- `src/backend/utils/cache/backend_runtime_cache.c` owns migrated
  cache/function-manager accessors;
- `src/backend/utils/activity/backend_runtime_pgstat.c` owns migrated pgstat
  backend/session accessors;
- `src/backend/jit/backend_runtime_jit.c` owns provider-independent and
  LLVM-provider JIT session accessors.
- `src/backend/utils/misc/backend_runtime_guc.c` owns migrated GUC
  compatibility accessors.

Subsequent Phase 12 JIT work moved the provider-independent callback cache
and LLVM provider-private type/template/module/context cache into `PgSession`.
The LLVM provider cache was validated with an LLVM 21.1.8 build, including an
explicit JIT smoke that produced leader and parallel-worker JIT functions. The
same slice fixed the JIT IR memory-context switch to call
`PgCurrentMemoryContextRef()` instead of resolving the removed
`CurrentMemoryContext` global. The global-lifetime scan now reports 61
session-local declarations with zero new unclassified mutable globals.

`backend_runtime.c` should remain focused on root runtime construction,
current-object installation, process/thread symmetry, and top-level
adoption/reset orchestration. Future migrations should update
`MULTITHREADED_RUNTIME_OWNERS.tsv`, the lifecycle checker source set when
needed, and the adjacent owner file in the same commit.
