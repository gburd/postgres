# XTC_ORC Supervision Design (AGENTS_XTC item #7)

Design/analysis only.  No source, build, or test files are changed by this
document.  Scope: how PostgreSQL postmaster supervision (crash detection and
restart of backends/workers) could map onto libxtc's `xtc_orc` supervisor
trees, and the smallest safe first step for the fiber-backed backend layer.

Reference tree paths:
- PostgreSQL (this branch): `src/backend/postmaster/postmaster.c`,
  `src/backend/postmaster/pmchild.c`,
  `src/backend/postmaster/launch_backend.c`,
  `src/backend/postmaster/pg_xtc_carrier.c`.
- libxtc: `/home/gburd/ws/xtc/src/inc/xtc_orc.h`, `xtc_app.h`, `xtc_proc.h`;
  implementation `/home/gburd/ws/xtc/src/orc/sup.c`.

Header-vs-implementation note: `xtc_orc.h`'s banner comment still says only
`XTC_SUP_ONE_FOR_ONE` is implemented and the others return `XTC_E_NOSYS`.
That comment is stale.  `src/orc/sup.c` (the M10.5 supervisor) implements all
four strategies: the dispatch `switch` at `sup.c:327` handles
`XTC_SUP_ONE_FOR_ALL` (`__do_one_for_all`), `XTC_SUP_REST_FOR_ONE`
(`__do_rest_for_one`), and `XTC_SUP_SIMPLE_OFO`; only an unknown strategy
value returns `XTC_E_NOSYS` (`sup.c:390`).  `docs/ARCHITECTURE.md` confirms
"M10 supervisor (4 strategies + restart intensity)".  This document trusts the
implementation, not the header banner.

--------------------------------------------------------------------------------

## 1. PostgreSQL postmaster supervision inventory

### 1.1 PMChild lifecycle (`pmchild.c`)

The postmaster tracks every child in a `PMChild` struct drawn from a fixed
per-type pool.  Pools are sized in `InitPostmasterChildSlots()`
(`pmchild.c`): `B_BACKEND` gets `2 * (MaxConnections + max_wal_senders)`,
`B_BG_WORKER` gets `max_worker_processes`, `B_IO_WORKER` gets
`MAX_IO_WORKERS`, and the singletons (`B_STARTUP`, `B_CHECKPOINTER`,
`B_BG_WRITER`, `B_WAL_WRITER`, `B_AUTOVAC_LAUNCHER`, `B_ARCHIVER`,
`B_LOGGER`, ...) get a pool of one.  Dead-end backends are the exception:
unbounded, `palloc`'d in `AllocDeadEndChild()`, not assigned a `child_slot`.

Lifecycle of a slot:
- `AssignPostmasterChildSlot(btype)` pops a free slot, sets
  `carrier_kind = PM_CHILD_CARRIER_PROCESS`, links it into `ActiveChildList`,
  and calls `MarkPostmasterChildSlotAssigned()` to mirror it into the shared
  `PMChildFlags` array (managed by `pmsignal.c`).
- The child is bound to a carrier by one of:
  `PostmasterChildSetProcess(pmchild, pid)` (fork),
  `PostmasterChildSetThread(pmchild, thread)` (a joinable pthread), or
  `PostmasterChildSetPooledLogical(pmchild)` (a fiber/pooled logical backend
  with no dedicated joinable carrier).  This sets `carrier_kind` to
  `PM_CHILD_CARRIER_PROCESS`, `PM_CHILD_CARRIER_THREAD`, or
  `PM_CHILD_CARRIER_POOLED_LOGICAL`.
- On exit the slot is returned to its pool by `ReleasePostmasterChildSlot()`,
  which unlinks it from `ActiveChildList` and calls
  `MarkPostmasterChildSlotUnassigned()`.  Its return value is the "did the
  child detach cleanly from shared memory" signal that `CleanupBackend()`
  turns into a crash if false.

Carrier-kind predicates: `PostmasterChildIsProcess`, `PostmasterChildIsThread`,
`PostmasterChildIsPooledLogical`, and
`PostmasterChildHasLogicalBackendPublication` (true for thread OR pooled
logical).  The publication surface (`logical_backend`, `logical_signal_pid`,
`thread_exitstatus`, `thread_startup_complete`, `thread_exited`) is the only
cross-thread channel between a logical backend and the postmaster, guarded by
`PMChildLogicalBackendMutex` and documented as the "Thread-backed PMChild
ownership contract" at the top of `pmchild.c`: the publisher writes the
payload, issues a memory barrier, sets the flag, and wakes the postmaster;
the postmaster owns all list mutation and slot release.

### 1.2 The postmaster state machine (`postmaster.c`)

`PMState` (`postmaster.c:337`) is the supervision state machine:

    PM_INIT -> PM_STARTUP -> {PM_RECOVERY -> PM_HOT_STANDBY ->} PM_RUN
      -> PM_STOP_BACKENDS -> PM_WAIT_BACKENDS
      -> PM_WAIT_XLOG_SHUTDOWN -> PM_WAIT_XLOG_ARCHIVAL
      -> PM_WAIT_IO_WORKERS -> PM_WAIT_CHECKPOINTER
      -> PM_WAIT_DEAD_END -> PM_NO_CHILDREN

Key states for supervision:
- `PM_RUN` / `PM_HOT_STANDBY`: normal running; new `B_BACKEND` children are
  allowed (`canAcceptConnections`).  In other states connection requests are
  answered by dead-end children.
- `PM_STOP_BACKENDS`: transient; means the same as `PM_WAIT_BACKENDS` but
  signals children first (shared code in `PostmasterStateMachine`).
- `PM_WAIT_BACKENDS`: wait for every regular backend, autovac
  launcher/workers, and bgworkers to exit.  Its exit condition is computed
  from a `BackendTypeMask` (`PostmasterStateMachine`, `postmaster.c:3127`):
  ends when `CountChildren(targetMask) == 0`.  During crash recovery or
  immediate shutdown the mask also includes checkpointer, archiver, IO
  workers, and walsenders.
- `PM_WAIT_DEAD_END`: wait for dead-end children to drain (they hold shared
  memory attachments that would block shmem re-creation).
- `PM_NO_CHILDREN`: all important children gone; the postmaster either exits
  (shutdown) or reinitializes for a crash-recovery restart cycle.

Two module-scope flags qualify "why" a post-`PM_RUN` state was entered:
`Shutdown` (smart/fast/immediate) and `FatalError` (recovering from a backend
crash).  `FatalError` is never true in `PM_RECOVERY`/`PM_HOT_STANDBY`/`PM_RUN`.

### 1.3 Crash detection (three reap paths)

The postmaster main loop (`ServerLoop`, dispatch at `postmaster.c:1731-1778`)
drives three separate reap paths every iteration:

1. `process_pm_child_exit()` (`postmaster.c:2364`): the process-carrier
   reaper.  `handle_pm_child_exit_signal` (SIGCHLD) sets
   `pending_pm_child_exit`; the loop then drains `waitpid(-1, ..., WNOHANG)`.
   For each dead pid it dispatches by matching against the known singleton
   `PMChild*` pointers (`StartupPMChild`, `BgWriterPMChild`,
   `CheckpointerPMChild`, `WalWriterPMChild`, ...), against
   `maybe_reap_io_worker(pid)`, and finally
   `FindPostmasterChildByPid(pid)` -> `CleanupBackend()`.  Under the threaded
   carrier this path also runs unconditionally when
   `multithreaded && PostmasterThreadCarriersStarted()` (`postmaster.c:1771`),
   because there is no SIGCHLD for an in-process thread/fiber death.

2. `process_pm_thread_exit()` (`postmaster.c:2610`): the joinable-thread
   reaper.  Scans `ActiveChildList`; for each entry whose
   `PostmasterChildHasExitedThread()` returns true it joins the native carrier
   via `PostmasterChildJoinThread()` (`pg_thread_join`), then routes by
   `bkend_type` to the per-role cleanup or `CleanupBackend()`.  If the join
   fails it calls `PostmasterChildRetryThreadExit()` to keep the slot and
   re-arm the exit report for a later loop.

3. `process_pm_pooled_logical_exit()` (`postmaster.c:2570`): the fiber /
   pooled-logical reaper -- the one the xtc carrier already uses.  Scans
   `ActiveChildList`; for each entry whose
   `PostmasterChildHasExitedPooledLogical()` returns true it warns on retained
   `TopMemoryContext` bytes and calls `CleanupBackend(pmchild, exitstatus)`.
   There is NO join, because a fiber has no dedicated joinable pthread; the
   exit was published by `PostmasterChildPublishPooledLogicalExit()` from
   inside the fiber (see `launch_backend.c:1561`, `1631`).

All three end by calling `PostmasterStateMachine()` if they reaped anything.

### 1.4 Crash -> restart-whole-cluster policy

`CleanupBackend()` (`postmaster.c:2762`) classifies a child exit: exit code 0
(normal) or 1 (FATAL) is clean; anything else sets `crashed = true`.  A
non-clean detach from shared memory (`ReleasePostmasterChildSlot` returns
false) is also treated as a crash.  On crash it calls
`HandleChildCrash(bp_pid, exitstatus, procname)`.

`HandleChildCrash()` (`postmaster.c:2989`):
- If already `FatalError` or `ImmediateShutdown`, it only updates
  bookkeeping and returns.
- IMPORTANT threaded-carrier special case (`postmaster.c:3014`): when
  `multithreaded && PostmasterThreadCarriersStarted()`, a child crash means
  the postmaster's own address space may be corrupt, so it logs
  "terminating threaded server runtime after child crash" and calls
  `ExitPostmaster(1)`.  There is no in-process crash-recovery cycle for a
  threaded carrier; the whole runtime dies and an outside supervisor/user
  restarts a clean postmaster.
- Otherwise (classic process mode) it calls
  `HandleFatalError(PMQUIT_FOR_CRASH, true)`.

`HandleFatalError()` (`postmaster.c:2906`):
- `TerminateChildren(SIGQUIT or SIGABRT)` to signal every other child to
  quickdie.
- Sets `FatalError = true`.
- Advances `pmState` to `PM_WAIT_BACKENDS` (or straight to `PM_WAIT_DEAD_END`
  if already mid-shutdown).
- Starts the SIGKILL clock: `AbortStartTime = time(NULL)`.

The "recalcitrant children" escalation is in the main loop
(`postmaster.c:1840`): if `(Shutdown >= ImmediateShutdown || FatalError)` and
more than `SIGKILL_CHILDREN_AFTER_SECS` (5s) have passed since
`AbortStartTime`, it logs "issuing SIGKILL/SIGABRT to recalcitrant children"
and calls `TerminateChildren(SIGKILL or SIGABRT)`.

Whole-cluster crash recovery: after all children are gone
(`PM_NO_CHILDREN`) with `FatalError` set, the postmaster reinitializes shared
memory and re-enters `PM_STARTUP` (`PostmasterStateMachine`,
`postmaster.c:3380-3452`), i.e. shutdown-then-startup.  This is
process-mode-only; the threaded path short-circuits to `ExitPostmaster` above.

### 1.5 Restartable vs. full-crash-recovery children

- Restarted individually on normal/expected exit, no cluster restart:
  auxiliary singletons (bgwriter, checkpointer, wal writer, wal receiver, wal
  summarizer, autovac launcher, archiver, syslogger, slot sync worker) --
  each has a per-role `cleanup_*_child` that, on a clean exit, just releases
  the slot and lets the main loop's `LaunchMissingBackgroundProcesses()` start
  a fresh one.  IO workers: `maybe_reap_io_worker` + `maybe_start_io_workers`.
  Background workers: restarted per `bgw_restart_time` policy
  (`rw_crashed_at`, `HaveCrashedWorker`).
- Force a full crash-recovery cycle when they die abnormally: ANY child that
  is attached to shared memory and exits non-clean.  A crashing backend, a
  crashing aux process, or an abnormal bgworker exit all route through
  `HandleChildCrash` -> `HandleFatalError`, because a process that died with a
  possibly-torn shared-memory write cannot be trusted; the only safe recovery
  is to kill everyone and re-init shared state.
- Never restarted: dead-end backends (they exist only to reject a connection).
- Threaded carrier: there is no "restart one, keep the rest" for an abnormal
  fiber/thread crash -- `HandleChildCrash` escalates to `ExitPostmaster(1)`.

--------------------------------------------------------------------------------

## 2. libxtc `xtc_orc` API inventory

### 2.1 Child specs (`xtc_orc.h`)

`xtc_child_spec_t`:
- `name` (logs), `fn` (`xtc_proc_fn` entry), `arg`.
- `policy` (`xtc_restart_policy_t`): `XTC_RESTART_PERMANENT` (always restart),
  `XTC_RESTART_TRANSIENT` (restart only on abnormal exit, i.e. non-zero
  reason), `XTC_RESTART_TEMPORARY` (never restart).  The decision lives in
  `__should_restart` (`sup.c:82`).
- `mailbox_cap` (0 = default), `loop` (executor loop index to place the child
  on when the supervisor owns an `xtc_exec`).

### 2.2 Supervisor options and strategies

`xtc_sup_opts_t`:
- `strategy` (`xtc_restart_strategy_t`): `XTC_SUP_ONE_FOR_ONE`,
  `XTC_SUP_ONE_FOR_ALL`, `XTC_SUP_REST_FOR_ONE`, `XTC_SUP_SIMPLE_OFO`.  All
  four are implemented in `sup.c` (see the header-vs-implementation note in
  the intro).
- `max_restarts` (default 3), `period_ns` (default 5s): restart intensity.
- `exec`: optional multi-loop executor; if set, children are placed across its
  loops (per `spec.loop`) and `xtc_exec_stop` fires when the supervisor exits.

Strategy behavior (from `sup.c`):
- ONE_FOR_ONE: on a child DOWN, respawn only that child (`__spawn_child`).
- ONE_FOR_ALL (`__do_one_for_all`): kill all siblings, respawn all.
- REST_FOR_ONE (`__do_rest_for_one`): kill and respawn the dead child and all
  children after it (in spec order); earlier children untouched.
- SIMPLE_OFO: dynamic-only pool; static children behave one-for-one, and
  `xtc_sup_add_child` grows the pool on demand.

Restart intensity: the supervisor records each restart timestamp
(`__record_restart`, `sup.c:59`) in a sliding window and, before respawning,
checks `__intensity_exceeded` (`sup.c:94`): if more than `max_restarts`
restarts fall inside `period_ns`, the supervisor stops restarting and EXITS
UP THE TREE (`break` out of the recv loop, `sup.c:322`).  On its own exit it
`xtc_exit_pid`s any still-alive children and, if it owns an `exec`, stops the
executor.  This "give up and propagate" is the OTP escalation analog.

### 2.3 API surface

`xtc_sup_start(loop, opts, children, n_children, &sup)` spawns the supervisor
as its own `xtc_proc`.  `xtc_sup_add_child(sup, spec, &pid)` adds a dynamic
child (must be called from within a proc; it awaits the supervisor's reply).
`xtc_sup_stop(sup)` is non-blocking and idempotent from any thread.
`xtc_sup_join(sup, timeout_ns)` waits for the supervisor to actually exit and
frees the handle.  Introspection: `xtc_sup_n_children`, `xtc_sup_n_restarts`,
`xtc_sup_alive`.

`xtc_app` (`xtc_app.h`) is the root-supervisor + registry container:
`xtc_app_create` / `xtc_app_start` (root sup with `n` children) /
`xtc_app_run` (blocks until the sup exits) / `xtc_app_stop`.  The xtc carrier
already builds an `xtc_app` with `n_loops = CPU count`
(`pg_xtc_carrier.c:182`), but starts it with ZERO children
(`xtc_app_start(g_xtc_app, NULL, 0)`, `pg_xtc_carrier.c:197`) and spawns each
backend fiber directly with `xtc_proc_spawn` (`pg_xtc_carrier.c:258`) rather
than as a supervised child.  So the supervisor exists but supervises nothing
today.

### 2.4 Observing a supervised proc's death (`xtc_proc.h`)

- `xtc_monitor(target, &ref)`: unidirectional.  When `target` exits the
  watcher receives a DOWN message
  `{ uint8_t kind='D'; uint64_t ref; xtc_pid_t pid; int reason; }`, decoded
  with `xtc_down_decode`.  The supervisor uses exactly this: `__spawn_child`
  spawns then `xtc_monitor`s the child (`sup.c`), and the recv loop reaps 'D'
  messages.  Monitoring does NOT kill the watcher when the target dies.
- `xtc_link(other)` / `xtc_unlink`: bidirectional fate-sharing (an exit
  propagates an EXIT to the linked peer).
- `xtc_exit_pid(target, reason)`: asynchronous cross-proc kill; the target
  raises the exit at its next yield/recv with `reason`.  `xtc_exit_self`
  exits the caller.
- Fault containment (R1): `xtc_fault_guard_install` + `xtc_proc_recovery_arm`
  turn a real SIGSEGV/SIGBUS/SIGFPE/SIGILL inside one fiber into an unwind of
  only that fiber, delivering DOWN to its monitors -- BUT only when the fault
  is outside a critical section.  `xtc_proc_critical_enter/leave` mark
  regions where a fault escalates to process abort (mirrors PG's
  `START_CRIT_SECTION`).  A contained fault does not release the fiber's
  locks/fds/allocations automatically; the recovery block (or
  `xtc_proc_recovery_track_*` + `xtc_proc_recovery_cleanup`) must.

--------------------------------------------------------------------------------

## 3. Mapping table: PostgreSQL concept -> nearest xtc_orc concept

Verdict legend: replace / wrap / keep (semantics diverge, stay PG) / defer.
"Guard" = the check that would catch a wrong equivalence assumption.

| PG supervision concept | Nearest xtc_orc concept | Verdict | Guard against a wrong assumption |
|---|---|---|---|
| Postmaster process (control plane) | `xtc_app` root supervisor / `xtc_sup` | keep (process-shaped) | AGENTS.md process-lifetime exception. Guard: TAP asserting a single postmaster OS process with the same crash-escalation behavior; any attempt to run the postmaster loop *as* an xtc proc must fail this. |
| `PMState` machine (PM_RUN..PM_NO_CHILDREN) | supervisor recv-loop states | keep | PG's states encode shmem-detach + WAL-shutdown ordering xtc has no notion of. Guard: `PM_WAIT_BACKENDS` mask test (`CountChildren(targetMask)==0`) must still gate shutdown. |
| `PMChild` slot + `ActiveChildList` | supervisor `children[]` array | keep + mirror | The PMChild pool also mirrors into shared `PMChildFlags` (pmsignal.c) for signal routing; xtc children do not. Guard: `MarkPostmasterChildSlot(Un)assigned` must still run for every fiber child. |
| `B_BACKEND` fiber carrier (this branch) | supervised `xtc_child_spec_t` under the carrier `xtc_app` | wrap (staged) | Fibers are spawned via bare `xtc_proc_spawn` today (`pg_xtc_carrier.c:258`), bypassing the sup. Guard: `xtc_sup_n_children` must equal the count of live pooled-logical PMChild slots. |
| SIGCHLD reaper `process_pm_child_exit` | (n/a for in-process fibers) | keep | A fiber death is not a SIGCHLD; the loop already runs the reapers unconditionally under threaded carriers (`postmaster.c:1771`). Guard: reaper must remain driven by the postmaster latch, not by SIGCHLD. |
| `process_pm_pooled_logical_exit` reaping | `xtc_monitor` DOWN observation | wrap (add DOWN as the trigger) | Today the fiber self-publishes exit via `PostmasterChildPublishPooledLogicalExit`; a monitor observes DOWN independently. Guard: exactly-once reaping -- DOUBLE-reap must be impossible (see Risks). |
| `PostmasterChildPublishPooledLogicalExit` | DOWN message reason field | keep (publish) + wrap (observe) | The publish path carries `exitstatus` + retained-memory bytes; a DOWN `reason` is a single int. Guard: xtc DOWN must not replace the publish payload, only corroborate it. |
| `CleanupBackend` / `ReleasePostmasterChildSlot` | supervisor slot free + optional respawn | keep | PG couples slot release with a shmem-clean-detach check that becomes a crash if false. Guard: `ReleasePostmasterChildSlot` returning false must still force `crashed`. |
| Normal backend exit (no restart) | `XTC_RESTART_TEMPORARY` | wrap | A client backend must NOT be auto-restarted (its client is gone). Guard: a fiber that exits code 0 must not be respawned; assert `xtc_sup_n_restarts` unchanged. |
| Abnormal backend/thread crash | `XTC_RESTART_*` respawn + intensity | KEEP -- do NOT restart | PG cannot trust shared memory after an in-process crash; it kills the whole runtime (`HandleChildCrash` -> `ExitPostmaster(1)`, `postmaster.c:3014`). Guard: the supervisor MUST NOT silently respawn a crashed backend; the whole-cluster policy must win. |
| `HandleFatalError` (SIGQUIT all, PM_WAIT_BACKENDS) | restart-intensity giveup -> sup exits up-tree | keep | xtc's giveup stops restarts and exits the sup; PG's must additionally re-init shmem or kill the runtime. Guard: `FatalError` path must still drive `TerminateChildren`. |
| "SIGKILL to recalcitrant children" (5s) | `xtc_exit_pid` on sup exit | keep | xtc's forced-kill has no grace timer; PG's `SIGKILL_CHILDREN_AFTER_SECS` is load-bearing for stuck backends. Guard: the 5s escalation must survive. |
| Whole-cluster crash recovery (re-init shmem, PM_STARTUP) | (no analog) | keep | xtc supervision restarts a fiber in place; PG must rebuild shared state. Guard: crash under threaded carrier must reach `ExitPostmaster(1)`, not an in-place fiber respawn. |
| Auxiliary singletons (checkpointer, bgwriter, ...) restart | `XTC_SUP_ONE_FOR_ONE` + `XTC_RESTART_PERMANENT` | defer (Phase 16/AGENTS_XTC #5) | These still fork or run on base pthreads, not on xtc. Guard: keep them off xtc until item #5; do not model them here. |
| Background workers (`bgw_restart_time`) | supervisor restart intensity | defer | Third-party bgworkers may be process-only. Guard: worker-runtime opt-in metadata (AGENTS.md) required before any xtc supervision. |
| Postmaster / single-user / bootstrap / crash-escalation lifetime | (n/a) | keep (process exceptions) | AGENTS.md deliberate process-lifetime exceptions. Guard: none of these paths may become xtc procs. |

Deliberate process-lifetime exceptions (restated per AGENTS.md, section
"Working Assumptions"): single-user mode, bootstrap mode, frontend
command-line utilities, postmaster/control-plane process lifetime, and
crash-escalation paths (PANIC / whole-cluster restart) stay process-shaped.
Therefore `xtc_orc` supervision is a candidate ONLY for the in-runtime
worker/backend layer, NOT for replacing the postmaster itself.  The postmaster
remains the top-level supervisor; any xtc supervisor sits strictly below it,
inside the single postmaster process, and never owns cluster crash policy.

--------------------------------------------------------------------------------

## 4. Concrete staged plan

Guiding principle: the postmaster stays the authority on crash policy.  Any
xtc supervisor is an OBSERVER and slot-lifecycle helper for fiber-backed
backends, layered under the existing single postmaster process.  No stage
changes the whole-cluster crash-recovery policy, and no stage lets a fiber
crash be silently masked.

### Stage 0 (this document): analysis, no code.

Verdict inventory above; invariants below.  Validation: `git diff --check`
(doc-only, per AGENTS.md).

### Stage 1 (smallest safe first step): observe fiber death via `xtc_monitor`

Goal: make a backend fiber's death observable through the xtc supervisor tree
WITHOUT taking over reaping and WITHOUT changing crash policy.

Model: place backend fibers under the carrier's existing `xtc_app` root
supervisor as `XTC_RESTART_TEMPORARY` children (never auto-restart), OR --
simpler and lower-risk -- keep the direct `xtc_proc_spawn`
(`pg_xtc_carrier.c:258`) and just add an `xtc_monitor(pid, &ref)` on each
backend fiber so the carrier holds a DOWN watcher.  Prefer the second: it does
not move the spawn path and keeps the current pooled-logical reaping intact.

What the monitor does (and does NOT do):
- On a NORMAL fiber exit (the fiber already called
  `PostmasterChildPublishPooledLogicalExit` at `launch_backend.c:1561`), the
  DOWN is purely corroborative.  The postmaster still reaps via
  `process_pm_pooled_logical_exit` -> `CleanupBackend`.  The monitor path must
  NOT also reap -- it only accounts/logs.  (Exactly-once reaping stays with
  the postmaster; see Risks 5.2.)
- On an ABNORMAL fiber exit where the publish path did NOT run (the fiber
  faulted before `backend_thread_shutdown`), the DOWN with a non-zero reason is
  the ONLY signal that the fiber is gone.  Here the monitor's job is to make
  the loss LOUD: log the crash and drive the existing crash policy.  Concretely
  it must ensure the postmaster reaches `HandleChildCrash` for that slot --
  which under `multithreaded && PostmasterThreadCarriersStarted()` means
  `ExitPostmaster(1)` (`postmaster.c:3014`).  The monitor must NOT respawn the
  fiber.

Preserved by Stage 1:
- PANIC / abnormal crash still restarts (kills) the whole runtime:
  `HandleChildCrash` is unchanged; a fiber crash still routes to
  `ExitPostmaster(1)`.
- `PM_WAIT_BACKENDS` semantics: the postmaster still counts pooled-logical
  PMChild slots via its own reaper; the monitor adds no slot state.
- No fiber crash silently masked: a fiber that dies without publishing exit
  now has a DOWN watcher that logs and escalates, closing the gap where a
  faulted fiber could leave a `PMChild` slot occupied forever (the current
  concurrent-teardown lost-wakeup class of hang).

Invariants (Stage 1):
- I1: For every backend fiber, exactly one PMChild slot is reaped exactly once,
  by the postmaster, regardless of whether the DOWN arrives before, after, or
  instead of the publish.
- I2: A fiber DOWN with non-zero reason never results in a fiber respawn.
- I3: `xtc_sup_n_restarts()` (if the sup path is used) stays 0 for the backend
  layer.
- I4: The postmaster remains the only writer of `ActiveChildList` and the only
  caller of `ReleasePostmasterChildSlot`.

Tests (Stage 1) -- extend the runtime evidence in M16_XTC_CARRIER_FINDINGS.md:
- T1 (normal): N sequential + N concurrent backends on the loop pool; assert
  spawned == exited, every slot reaped once, monitor DOWN count == exit count,
  restart count 0.  (Builds on the already-verified 6-concurrent case.)
- T2 (crash): inject a SIGSEGV in one backend fiber (a debug-only fault hook
  under the carrier); assert the postmaster logs "terminating threaded server
  runtime after child crash" and calls `ExitPostmaster(1)` -- i.e. the crash is
  NOT masked and NOT locally respawned.
- T3 (shutdown): `pg_ctl -m fast stop` still completes (PM_WAIT_BACKENDS
  drains) with the monitor active.
- Guardrail targets when the touched surface is worker/wait-boundary shaped:
  `gmake check-threaded-workers`, `gmake check-threaded-world-core`
  (per AGENTS.md Validation Defaults).

### Stage 2 (later, defer): supervised backend spawn under the root sup

Only after Stage 1 is green and the concurrent lost-wakeup class is closed by
the loop pool.  Move backend spawn to `xtc_sup_add_child` under the carrier
`xtc_app` with `XTC_SUP_SIMPLE_OFO` + `XTC_RESTART_TEMPORARY`, so the
supervisor owns the monitor and the DOWN reap becomes the single source of
fiber-death truth.  The postmaster still owns PMChild slot release and crash
policy; the supervisor's respawn is disabled (TEMPORARY) for client backends.
Guard: I1-I4 unchanged; add a test that `xtc_sup_add_child` failure surfaces as
a backend-launch failure the postmaster handles, not a silent drop.

### Stage 3 (Phase 16 / AGENTS_XTC #5, defer): aux/bgworker supervision

Out of scope here.  Requires items #5 (aux processes on xtc) and worker-runtime
opt-in metadata (AGENTS.md).  Auxiliary singletons could eventually be
`XTC_SUP_ONE_FOR_ONE` + `XTC_RESTART_PERMANENT` children, but only once they
run on xtc at all.  Do not model until then.

--------------------------------------------------------------------------------

## 5. Risks

### 5.1 Restart-semantics divergence vs. PG's "crash one -> restart everything"

xtc_orc's default reflex (docs/guide/transitioning.md: "let it crash, let the
supervisor restart it from a known-good init") is the OPPOSITE of PG's
shared-memory invariant.  A `PERMANENT`/`TRANSIENT` child that crashes gets
respawned in place by the supervisor.  For a PG backend attached to shared
memory that is exactly wrong: a crashed backend may have left a torn write, a
held LWLock, or a corrupt buffer, so PG kills the WHOLE cluster and re-inits
shmem (`HandleChildCrash` -> `HandleFatalError`, or `ExitPostmaster(1)` under a
threaded carrier).  Mitigation: client backends MUST be `XTC_RESTART_TEMPORARY`
(never restart), and the supervisor's respawn must be inert for the backend
layer.  Guard: Stage-1 invariant I2/I3 and test T2 fail loudly if a crashed
backend is ever respawned by xtc instead of escalating to the postmaster.

### 5.2 Double-reaping (both xtc_orc and the postmaster reaper act)

Today the postmaster reaps a fiber exit exactly once via
`process_pm_pooled_logical_exit` -> `CleanupBackend` ->
`ReleasePostmasterChildSlot`.  If an xtc monitor/supervisor ALSO treats the
DOWN as a reap and frees the slot, the slot is double-freed (pushed twice onto
the pool freelist) -- silent corruption of the PMChild pool, and later a wrong
slot handed to a new backend.  Mitigation: the xtc monitor is observe-only for
normal exits; ONLY the postmaster mutates `ActiveChildList` and calls
`ReleasePostmasterChildSlot` (invariant I4).  The publish flag
(`thread_exited`, cleared by `pg_atomic_exchange_u32` in
`PostmasterChildHasExited`) is the exactly-once latch; the DOWN must not have
its own slot-release path.  Guard: an assertion / test that each `child_slot`
is released exactly once per generation (T1), and that
`PostmasterChildHasExitedPooledLogical` is the sole gate to `CleanupBackend`.

### 5.3 Shared-memory corruption after a fiber crash (why PG restarts all)

A fiber shares the postmaster's address space (that is the whole point of the
threaded runtime).  A SIGSEGV in one fiber can already have scribbled on
another session's memory or shared buffers before it faulted.  xtc's R1 fault
containment (`xtc_proc_recovery_arm`) can unwind the ONE faulting fiber's
stack, but it explicitly does NOT prove the rest of the address space is
intact, and it escalates to process abort inside a critical section
(`xtc_proc_critical_enter`, mirroring `START_CRIT_SECTION`).  This is why
`HandleChildCrash` under a threaded carrier does NOT attempt in-process
recovery and instead `ExitPostmaster(1)`s.  Risk: someone wires xtc fault
containment as a "recover and keep serving" path, masking corruption.
Mitigation: keep the threaded-carrier crash escalation; use containment only to
guarantee a clean DOWN + resource release on the way down, never to continue
the runtime.  Guard: test T2 asserts the runtime terminates on an injected
fiber crash; a regression that keeps the postmaster alive fails it.

### 5.4 Interaction with existing pooled-logical reaping

The carrier already classifies backend fibers as
`PM_CHILD_CARRIER_POOLED_LOGICAL` and reaps them via
`PostmasterChildPublishPooledLogicalExit` / `process_pm_pooled_logical_exit`
(the shutdown-hang fix in M16_XTC_CARRIER_FINDINGS.md).  Adding xtc monitoring
must not disturb this: the publish path stays the primary, exactly-once reap
trigger; the DOWN is secondary (corroborate + escalate-on-crash).  Risk: a race
where the DOWN fires before the publish flag is visible could tempt a shortcut
that reaps on DOWN, re-introducing 5.2.  Mitigation: DOWN never reaps; it only
logs/accounts on a matched publish, or escalates when NO publish arrives.
Guard: memory-ordering test that publish (`pg_memory_barrier` +
`thread_exited=1`) always wins the reap, and that a DOWN-without-publish path is
routed to crash escalation, not to slot release.

### 5.5 Timer/ownership hazards specific to the carrier

The supervisor is itself an `xtc_proc` on a loop; `xtc_sup_stop` was noted in
`sup.c` to have deadlocked when called from a proc on the same loop.  A carrier
supervisor placed on loop 0 while backends run on loops 1..N-1 avoids that, but
`xtc_sup_join` must be called from OUTSIDE the sup's loop thread (the
postmaster main thread qualifies; a carrier loop thread does not).  Guard: any
`xtc_sup_stop`/`xtc_sup_join` added in Stage 2 must be issued from the
postmaster thread, verified by a shutdown test that does not wedge the loop.

--------------------------------------------------------------------------------

## Appendix: cited symbols and locations

PostgreSQL (`src/backend/postmaster/`):
- `postmaster.c`: `PMState` enum (l.337); state-machine comment (l.293);
  SIGKILL-recalcitrant (l.1840); reaper dispatch in ServerLoop (l.1731-1778);
  `process_pm_child_exit` (l.2364); `process_pm_pooled_logical_exit` (l.2570);
  `process_pm_thread_exit` (l.2610); `CleanupBackend` (l.2762);
  `HandleFatalError` (l.2906); `HandleChildCrash` (l.2989) with the threaded
  `ExitPostmaster(1)` at l.3014; `PostmasterStateMachine`/`PM_WAIT_BACKENDS`
  mask (l.3101-3175); crash-recovery re-init to PM_STARTUP (l.3380-3452).
- `pmchild.c`: `InitPostmasterChildSlots`; `AssignPostmasterChildSlot`;
  `AllocDeadEndChild`; carrier-kind predicates; the logical publication
  contract; `PostmasterChildPublishPooledLogicalExit`;
  `PostmasterChildHasExitedPooledLogical`; `ReleasePostmasterChildSlot`;
  `FindPostmasterChildByPid`.
- `launch_backend.c`: `USE_XTC_CARRIER` B_BACKEND route (l.605-638);
  `xtc_pg_launch_backend_fiber` call (l.616); pooled-logical publish on exit
  (l.1561, l.1631).
- `pg_xtc_carrier.c`: `xtc_app_create` with `n_loops` (l.182-197); backend
  fiber spawn via bare `xtc_proc_spawn` (l.258).

libxtc:
- `src/inc/xtc_orc.h`: strategies, `xtc_child_spec_t`, `xtc_sup_opts_t`, API.
- `src/inc/xtc_app.h`: root supervisor / app lifecycle.
- `src/inc/xtc_proc.h`: `xtc_monitor`/`xtc_link`/`xtc_exit_pid`/`xtc_down_decode`;
  R1 fault containment (`xtc_proc_recovery_arm`, critical sections).
- `src/orc/sup.c`: `__should_restart` (l.82); `__intensity_exceeded` (l.94);
  `__spawn_child` + `xtc_monitor` (l.119+); strategy dispatch (l.327);
  strategy validation / `XTC_E_NOSYS` for unknown (l.390); `xtc_sup_start`.
- `docs/ARCHITECTURE.md`: "M10 supervisor (4 strategies + restart intensity)".
