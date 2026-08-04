# Phase 19 Increment 4 design: clean mid-command unwind + process-fallback re-placement (2026-08-05)

Design-only pass (no code). Source tree at branch `xtc`, HEAD `2b41b4883f4`.

Inc 4: when a pooled-carrier session hits an incompatible (process-only) module
MID-COMMAND (`LOAD`, `CREATE EXTENSION`, first fmgr call into the .so), instead
of the current fail-closed `ERROR`, cleanly abort the uncommitted command and
re-place the session as a fork+exec process-fallback backend -- ideally without
the client seeing a hard error.

## Prerequisite state (what Inc 1-3 already built, verified)

- **Inc 1 (classification)**: `incompatible_module_backend_model_error`
  (`dfmgr.c` ~613) distinguishes process-only modules from mis-declared-weaker
  ones, with an actionable operator message. This is the branch point.
- **Inc 2 (fork+exec route)**: `postmaster_pooled_protocol_process_fallback`
  (`launch_backend.c` ~1063) + the `PgRuntimeGetBackendLaunchModel` /
  process-fallback route from `postmaster_pooled_protocol_launch` (~1104),
  behind the `xtc_force_process_fallback` knob. `internal_forkexec(B_BACKEND,
  ...)` produces a clean exec'd child that arrives in `SubPostmasterMain`,
  re-attaches shmem, restores backend vars, and runs as a normal supervised
  process backend. Requires `shared_memory_type=sysv` (an exec'd child cannot
  re-attach anonymous mmap).
- **Inc 3 (startup detection)**: process-only `shared_preload_libraries` are
  re-checked and REJECTED at startup under multithreaded=on.
- **Test knob + TAP**: `xtc_force_process_fallback` (a `PGC_POSTMASTER` GUC,
  `globals.c` / `guc_parameters.dat`) forces EVERY pooled client backend down
  the fork+exec route at LAUNCH time; TAP 013
  (`013_phase19_process_fallback_crash.pl`) pins the fork+exec route's
  crash/fail-stop contract. This is the PRE-command route -- the session never
  runs on a carrier at all.

## Two KEY CORRECTIONS this design is built around (both verified in source)

1. **Not a continuation-capture problem.** With STACKFUL fibers the session's C
   stack IS the fiber; there is nothing to "capture." The old Phase-17
   stackless-model framing is wrong for the shipped runtime.

2. **The carrier-monopolizer is ALREADY fiber-aware.** The old Phase-17 note
   claimed `ProcWaitOnSemaphore` does a raw carrier-blocking `sem_wait`. That is
   STALE. Verified in `proc.c`: `ProcWaitOnSemaphore` (line 2361) checks
   `proc->sem_fiber_backed && proc->sem_wake_fd >= 0` and, for a fiber-backed
   proc, calls `ProcSemaphoreWaitFiber` (line 2301) which parks on the per-PGPROC
   eventfd via `xtc_pg_wait_fd(proc->sem_wake_fd, ...)` (line 2333) -- yielding
   the carrier. The eventfd is created per-PGPROC (line 333); `ProcWakeSemaphore`
   (line 2426) writes it for fiber-backed procs. So there is NO carrier-blocking
   deep-wait blocker left for Inc 4 to work around.

The decisive consequence of both: at `LOAD` / `CREATE EXTENSION` / first-fmgr,
the session has **committed nothing** (or is in a normal aborTABLE transaction),
holds a live but ordinary fiber stack, and every wait it can be in yields its
carrier. So the unwind is NOT novel machinery -- it is the STANDARD PostgreSQL
error unwind (`AbortCurrentTransaction`), with a different TERMINAL action:
instead of returning to the command loop to read the next message, hand the
session off to be re-placed as a process backend.

---

## Step 1 -- Where exactly to catch the incompatible-module condition

The precise hook is the SAME function Inc 1 built:
`incompatible_module_backend_model_error` (`dfmgr.c` ~613). Every mid-command
path into an incompatible module funnels through it:

- `internal_load_library` (`dfmgr.c` ~331): the `LOAD` / first-time dlopen path,
  after reading the magic block and finding `PG_BACKEND_MODEL_PROCESS`
  incompatible with the carrier's required model.
- `check_module_backend_model` (`dfmgr.c` ~592, called from the already-loaded
  path ~604): a session that re-references a module.

`CREATE EXTENSION` and first-fmgr both bottom out in `internal_load_library` ->
this function. So there is ONE catch site, and it is already the classification
site -- the lazy fix is here, not scattered across LOAD/CREATE-EXTENSION/fmgr
callers.

The catch is a two-way branch INSIDE (or just before) this function:

- If the session is a pooled/carrier fiber (`PgRuntimeIsThreadBacked` +
  `xtc_in_backend_fiber` / a carrier-session predicate) AND the module is the
  re-placeable class (`module_backend_model == PG_BACKEND_MODEL_PROCESS`, the
  process-only default) AND process-fallback is available
  (`shared_memory_type == sysv`, `USE_XTC_PROCESS_FALLBACK`, non-Windows) ->
  raise a DISTINCT internal condition that the unwind path recognizes as
  "re-place, do not fail."
- Otherwise -> the existing fail-closed `ereport(ERROR)` (mis-declared modules,
  mmap shmem, process mode, non-carrier).

**Design decision (Step 1): use a distinct errcode, not a new longjmp.** Raise
`ereport(ERROR, (errcode(ERRCODE_XTC_NEEDS_PROCESS_FALLBACK), ...))` -- a normal
ERROR that unwinds through the EXISTING `sigsetjmp` recovery in `PostgresMain`,
but carries a recognizable errcode. This reuses the entire proven abort machinery
(no new unwind path, no new longjmp target, no risk of a half-cleaned session)
and lets the recovery block decide the terminal action by inspecting the errcode.
Adding a whole new internal signal/longjmp would duplicate the abort cleanup and
is the riskier path -- rejected.

---

## Step 2 -- Cleanly abort the current command/transaction without committing

**Reuse the existing error-recovery path verbatim.** `PostgresMain`'s
`sigsetjmp(local_sigjmp_buf, ...)` block (`postgres.c` ~5491 / the pooled
equivalent ~7080) already does the complete uncommitted-command unwind on ANY
ERROR:

- `error_context_stack = NULL`, `HOLD_INTERRUPTS`, `disable_all_timeouts`,
  `QueryCancelPending = false`, `pq_comm_reset`;
- `AbortCurrentTransaction()` (~4855) -- rolls back the uncommitted transaction,
  releases locks, releases buffer pins, releases the resource owner, fires
  reset callbacks;
- `PortalErrorCleanup`, replication-slot release, etc.

Because the session committed nothing at `LOAD`/`CREATE EXTENSION`/first-fmgr,
this abort leaves NOTHING durable behind -- exactly the clean state the handoff
needs. This branch's own comments (postgres.c ~5498) already note the abort path
may legitimately PARK (contended-lock ProcSleep, pq_flush backpressure) and the
fiber handles that -- so the abort is already fiber-safe.

**The only NEW logic** is at the tail of the recovery block: after the standard
cleanup, check `geterrcode() == ERRCODE_XTC_NEEDS_PROCESS_FALLBACK` (captured
before `EmitErrorReport` frees the error). If set, take the handoff branch
(Step 3) instead of falling through to the normal "return to the command loop"
tail. This is a single conditional at the bottom of an already-existing block.

**Suppress the client-facing ERROR.** Normally the recovery block calls
`EmitErrorReport()` (~4838) which sends the ErrorResponse to the client. For the
re-placement case we must NOT send a hard error (goal: reconnect-transparent). So
the re-placement branch skips `EmitErrorReport` to the client (still logs
server-side at LOG/DEBUG), and proceeds to the handoff. This is the one place
the standard path is diverged.

---

## Step 3 -- Hand the session off for fork+exec re-placement (fd handoff, no client reconnect)

### The good news: the client fd is already inheritable

A pooled session holds its client socket in `logical_start->client_sock.sock`
(`launch_backend.c` ~1819, `MyClientSocket = &logical_start->client_sock`). That
fd was `dup()`'d from the accepted socket in `postmaster_pooled_protocol_launch`
(~1169) and lives in the POSTMASTER process's fd table (carriers run inside the
postmaster process). On POSIX, `write_inheritable_socket` is a no-op macro that
just copies the fd number (`launch_backend.c` ~3137); the exec'd child inherits
the open fd across fork+exec (fd is not CLOEXEC on the inherited path). This is
EXACTLY how the pre-command fork+exec route already re-attaches the live client
socket -- `save_backend_variables` -> `internal_forkexec` -> child
`read_inheritable_socket` (~3418) -> `MyClientSocket->sock`.

**So the client does NOT need to reconnect.** The live, mid-protocol client fd
can be handed to the process-fallback backend the same way the pre-command route
already hands the freshly-accepted fd. The protocol state matters (see open
questions) but the SOCKET itself transfers.

### The mechanism: signal the postmaster to fork+exec, then park/exit the fiber

Carriers cannot `fork()` safely (the postmaster is multithreaded once carriers
exist -- a bare fork inherits locked sibling mutexes; this is the whole reason
Inc 2 uses fork+EXEC and the postmaster does the forking). So the fiber cannot
itself spawn the process backend. It must ask the postmaster.

The handoff, step by step:

1. **Fiber side (in the recovery-block re-placement branch):**
   - The transaction is already aborted (Step 2); locks/pins/resource-owner
     released; no shmem left inconsistent.
   - Build a re-placement request carrying: the `child_slot` / PMChild identity,
     the `client_sock` (fd + addr), the original `BackendStartupData` (startup
     packet, user/db), and the protocol phase (see open Q on protocol state).
   - Detach the fiber's client fd from close-on-fiber-exit: the fd must survive
     for the postmaster to pass it to the child. Today the fiber's exit path
     `closesocket(logical_start->client_sock.sock)` (~2509). The re-placement
     path must NOT close it -- transfer ownership to the request instead.
   - Enqueue the request to the postmaster (the postmaster already owns
     `internal_forkexec`; the natural channel is a PMSignal or the same
     postmaster-latch + a queue the launch path can drain, mirroring how
     `postmaster_pooled_protocol_launch` is itself driven from the postmaster
     thread). Then the fiber releases its PMChild logical publication and EXITS
     the fiber cleanly (a normal fiber exit, NOT a fault -> no fail-stop).

2. **Postmaster side:**
   - On the re-placement request, call
     `postmaster_pooled_protocol_process_fallback(pmchild, child_slot,
     &startup_data, sizeof, &client_sock)` -- the EXACT function Inc 2 built --
     which does `internal_forkexec(B_BACKEND, ...)`. The exec'd child inherits
     the client fd and arrives in `SubPostmasterMain` as an ordinary process
     backend attached to the same client socket.
   - The PMChild slot bookkeeping must be reconciled: the logical pooled backend
     is being replaced by a physical process backend on (ideally) the same slot,
     or a fresh slot with the old one released. This is the fiddly part (see
     open questions).

### Does the client see a hard error?

Goal: no. If the handoff happens BEFORE any command result byte was sent for the
current command (true at LOAD/CREATE EXTENSION/first-fmgr -- the module load is
the first thing the command does), the client is still waiting for the command
result. The process backend inherits the fd and, once it finishes startup +
loads the (now process-safe) module, completes the SAME command and sends the
result. The client sees a slower-than-usual response, not an error or a
disconnect.

The realistic fallback if seamless resume proves too hard (see open Q on
protocol/transaction resume): send the client a `ROLLBACK`-equivalent
notice/retryable error for the aborted command, but keep the CONNECTION alive on
the process backend so the client's next command runs there transparently. That
is "the current statement failed, retry it" -- weaker than fully seamless, but
still connection-transparent (no reconnect). This is the honest floor.

---

## Step 4 -- What state must be clean before handoff

`AbortCurrentTransaction()` (Step 2) already guarantees most of it. The explicit
checklist the handoff must assert:

- **Transaction aborted**: no open transaction, nothing committed
  (`AbortCurrentTransaction` done).
- **All heavyweight locks released**: `AbortCurrentTransaction` ->
  `ResourceOwnerRelease` releases locks. Assert `MyProc` holds no locks.
- **No held buffer pins**: released by resource-owner cleanup. Assert
  no pinned buffers.
- **No LWLocks held**: the abort path releases them; assert
  `LWLockReleaseAll`-clean (the recovery block context).
- **No fiber parked on the per-PGPROC sem eventfd / latch**: the fiber is running
  the recovery block, not parked -- clean by construction.
- **The half-loaded .so must be dlclose'd**: the incompatible module was NOT
  fully loaded (the check fires before `call_module_init_function`), and
  `internal_load_library` already `dlclose`s + frees on the incompatible path
  (`dfmgr.c` ~325, ~600). So no carrier-address-space contamination -- the whole
  point of catching it at the model check. Assert the module is NOT in
  `file_list`.
- **The client fd is transferred, not closed**: ownership moved to the
  re-placement request (Step 3); the fiber-exit close is suppressed.
- **PMChild publication reconciled**: the logical pooled backend
  publication must be handed to / released for the incoming process backend so
  the postmaster's slot accounting stays exactly-once.

---

## Concrete implementation plan (functions/files to touch)

1. **`src/backend/utils/errcodes.txt` (+ generated `utils/errcodes.h`)**: add
   `ERRCODE_XTC_NEEDS_PROCESS_FALLBACK` (a private/internal errcode). Small.
2. **`src/backend/utils/fmgr/dfmgr.c`
   (`incompatible_module_backend_model_error` + its callers)**: add the "carrier
   fiber + process-only module + fallback available" branch that raises the new
   errcode instead of the fail-closed message. Keep every other path fail-closed.
3. **`src/backend/tcop/postgres.c` (the `sigsetjmp` recovery block, ~5491 and
   the pooled variant ~7080)**: after the standard abort cleanup, if the pending
   error is `ERRCODE_XTC_NEEDS_PROCESS_FALLBACK`: skip the client
   `EmitErrorReport`, build the re-placement request, transfer the client fd,
   release the logical publication, enqueue to the postmaster, and exit the
   session fiber cleanly. One conditional tail branch.
4. **`src/backend/postmaster/launch_backend.c`**: a small
   postmaster-side entry that drains a re-placement request and calls the
   EXISTING `postmaster_pooled_protocol_process_fallback` with the transferred
   fd + startup data. Reconcile the PMChild slot.
5. **`src/backend/postmaster/postmaster.c` / pmsignal**: the signal/queue channel
   from a carrier fiber to the postmaster main loop (mirror how the pooled launch
   is already postmaster-driven).
6. **Test knob**: extend `xtc_force_process_fallback` (or add
   `xtc_force_process_fallback_midcommand`) so the fallback fires at the MODULE
   CHECK rather than at launch -- see testability.

Files touched: 4-5 (errcodes, dfmgr, postgres, launch_backend, postmaster).

---

## Risks (this is the riskiest of the three forward items)

- **xact-abort correctness**: reusing the standard recovery path is the LOW-risk
  choice, but the tail-branch must run AFTER the abort is fully complete and must
  not itself throw (it runs with `HOLD_INTERRUPTS`). A throw in the handoff
  branch would recurse the recovery. Mitigate: the handoff enqueue + fd transfer
  are simple, non-allocating, non-throwing operations; assert-guard them.
- **fd ownership double-close / leak**: the fd is currently closed at fiber exit
  (~2509). The transfer must move ownership atomically so it is closed EXACTLY
  once -- by the process backend (on its exit) OR by the postmaster (if
  fork+exec fails), never by the fiber. A double-close corrupts an unrelated fd;
  a leak exhausts fds. This is the sharpest correctness edge.
- **PMChild slot accounting**: replacing a logical pooled backend with a physical
  process backend on the postmaster's slot table is exactly-once-sensitive
  (roadmap's "PMChild logical-backend publication vs physical carrier lifetime"
  split). Getting it wrong double-counts or leaks a MaxConnections slot.
- **protocol/transaction resume** (the real depth): whether the command can be
  SEAMLESSLY resumed on the process backend, or whether the client must retry the
  statement (connection-transparent but not statement-transparent). See open
  questions.
- **fail-stop interaction**: the fiber must exit CLEANLY (normal fiber exit), not
  fault -- a fault triggers the whole-process fail-stop (Item A of the structural
  doc). The handoff branch must reach a clean `xtc_exit_self`, not a crash.
- **wake-race with a concurrent cancel/terminate**: a cancel arriving during the
  handoff must not leave the fiber half-handed-off. The recovery block already
  clears `QueryCancelPending`; the handoff must be a point of no return once the
  fd is transferred.

## Open questions

1. **Protocol phase resume.** At first-fmgr the client is mid-`Query`/`Execute`.
   Can the process backend resume the exact protocol state (re-run the command
   from the startup packet + the buffered command), or is the honest MVP "abort
   this statement, keep the connection, next statement runs on the process
   backend"? The startup packet re-runs cleanly (the pre-command route proves
   it); resuming an in-flight extended-protocol command is harder. RECOMMEND:
   MVP = re-run the command from scratch on the process backend for the simple
   (simple-Query) case; statement-retry for extended protocol. Measure demand
   before building seamless extended-protocol resume.
2. **CREATE EXTENSION atomicity.** `CREATE EXTENSION` runs in a transaction that
   the abort rolls back -- so after re-placement the client re-runs `CREATE
   EXTENSION` on the process backend and it succeeds there. Confirm no partial
   catalog state survives the abort (it should not -- that is what abort is for).
3. **Which side owns the fd on fork+exec failure.** If `internal_forkexec`
   fails, the postmaster must close the transferred fd and report the failure to
   the client (a real error at that point). Define this error path.
4. **Slot reuse vs fresh slot.** Reuse the logical backend's PMChild slot for the
   physical backend, or release + allocate fresh? Fresh is simpler to reason
   about (no in-place type change) but must not transiently exceed
   MaxConnections. RECOMMEND fresh slot + release-old, guarded against the
   MaxConnections window.
5. **The carrier-fiber -> postmaster channel.** Reuse an existing PMSignal, or a
   dedicated queue drained on the postmaster latch? The pooled launch path is
   already postmaster-thread-driven; the cleanest is a small request queue the
   postmaster drains, mirroring the launch enqueue.

## Testability plan

- **Deterministic mid-command trigger** (the analog of `xtc_force_process_fallback`
  for the LAUNCH route): add a knob that forces
  `incompatible_module_backend_model_error`'s re-placement branch at the MODULE
  CHECK. Two options:
  1. A developer GUC `xtc_force_process_fallback_midcommand` that makes the model
     check treat a NAMED test module (or any `LOAD`) as process-only-needing-
     re-placement even on a carrier -- firing the Step-1 branch on demand.
  2. Reuse the existing `test_backend_runtime` module machinery: a test .so
     marked `PG_BACKEND_MODEL_PROCESS` that, when `LOAD`ed by a carrier session,
     deterministically hits the incompatible-module check mid-command.
  Option 2 is closer to the real path (a genuine process-only module `LOAD`ed
  mid-session) and reuses the existing test extension; PREFER it, with option 1
  as the forcing knob if a real process-only test module is awkward to stage.
- **TAP 014 (the mid-command analog of TAP 013)**:
  - Start a pooled threaded server (`multithreaded=on`, sysv,
    fallback-mid-command enabled).
  - Connect a client, run a trivial command on a carrier (prove it is a carrier
    session).
  - `LOAD` / `CREATE EXTENSION` the process-only test module mid-session.
  - Assert: the connection stays UP (no reconnect), the command (or its retry)
    succeeds, and `pg_stat_activity` / a backend-type probe shows the session is
    now a PROCESS backend (fork+exec'd), not a carrier fiber.
  - Assert: committed data from before the handoff survives; the aborted
    command left no partial state.
  - Negative: on `shared_memory_type=mmap`, the SAME `LOAD` still fails closed
    with the actionable error (re-placement unavailable) -- the fail-closed path
    is preserved.
- **Reuse TAP 013's crash contract**: once re-placed, the process-fallback
  backend's crash behaviour is already pinned by 013 (fail-stop + committed-data-
  survives). 014 need only cover the HANDOFF, not re-test the crash contract.

## Bottom line: small glue or larger surface?

**Larger than "modest glue," but not a rewrite.** The prior triage's "abort +
re-place glue, not continuation-capture" framing is CORRECT about the hard part:
the unwind reuses the standard `AbortCurrentTransaction` recovery, and the client
fd is already inheritable, so there is no continuation capture and no client
reconnect. That is genuinely modest.

BUT three real surfaces make it more than a one-commit glue:
(1) the carrier-fiber -> postmaster re-placement channel + PMChild slot
reconciliation (new plumbing, exactly-once-sensitive);
(2) fd-ownership transfer with exactly-once close (the sharpest correctness
edge); and
(3) the protocol/transaction resume decision (seamless resume is a real project;
statement-retry is the honest MVP).

Honest sizing: the ABORT is glue (reuse the recovery path + one errcode + one
tail branch). The RE-PLACEMENT is a small new subsystem (channel + slot + fd
transfer) reusing Inc 2's fork+exec route. The RESUME is where scope can balloon
-- cap it at statement-retry for the MVP and defer seamless extended-protocol
resume until demand is proven. Sequenced that way, Inc 4 is a 2-3 commit
increment (errcode+catch; abort-tail+channel; fd-transfer+slot), each keeping the
fail-closed path intact as the fallback, plus TAP 014. It touches xact-abort and
session lifecycle -- the riskiest forward item -- so each commit must stay green
on check-threaded-pooled and preserve the Inc-1 fail-closed message on every path
the re-placement branch does not take.
