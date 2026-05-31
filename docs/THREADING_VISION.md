# PostgreSQL on xtc -- Threading Vision

**Status:** design / direction. Reviewable, durable.
**Worktree:** `~/ws/postgres/xtc` (branch `xtc`).
**Companion docs:**
- `threading/F1_CLASSIFICATION_HARNESS.md` -- the prerequisite tooling.
- `../spikes/f5_crash_containment.c` -- session crash containment proof.
- `../spikes/f7_satellite_bridge.c` -- extension satellite bridge proof.
- xtc tree: `docs/M16_PG_ADAPTER.md`, `docs/M_LIBXTC_PG_BOUNDARY.md`,
  `PLAN.md` section 20 (PG-workplan -> xtc-primitive map).

## Thesis

PostgreSQL keeps its crown jewels -- planner, executor, MVCC, WAL, the
wire protocol, the catalog *content*. What changes is the **substrate**:
the process model, the wait model, the memory substrate, the lock
primitives, and the failure model. We back those substrate slots with
**xtc** (a Tokio+Seastar+BEAM-shaped C runtime whose stated ultimate
consumer is a threaded PostgreSQL), behind PG's existing APIs, one
subsystem at a time.

The endpoint: PostgreSQL as a **share-nothing, supervised,
message-driven, budget-bounded actor system that speaks SQL** -- with
multi-process retained as the isolation fallback for un-converted
extensions.

The bright line (from `M_LIBXTC_PG_BOUNDARY.md`): **runtime concerns ->
xtc; database concerns -> PG core.** xtc must never learn what a buffer
cache, a WAL record, or a SQL query is.

---

## Part 1 -- The major thread-safety issues (evidence-based)

Counts from a clean `master` checkout, `src/backend`, 903 `.c` files.

1. **Process-global mutable state.** ~517 file-scope mutable statics;
   827 `PGDLLIMPORT` globals across 170 headers. The hard part is
   *classification*, not tagging: each global is per-session,
   set-once, genuinely-shared, or a hidden cache. (Heikki already
   classified 1187 as `session_local`, 133 as `postmaster_guc`.)

2. **Function-scope `static`.** 153, of which 104 have initializers.
   Invisible scratch/caches that corrupt across sessions with no
   compiler warning.

3. **Thread-unsafe libc.** `setlocale` x23, `getenv` x14, `setenv` x4,
   `rand`/`srand` x17, `strtok` x2, `localtime` x1, `getopt` x1. Each
   needs a thread-safe replacement (`uselocale`, frozen env,
   `random_r`, `strtok_r`, `localtime_r`, `pg_getopt`).

4. **The GUC system.** ~350 entries, each binding to `&a_global`. The
   storage *is* a process global. Move to a function-call API
   (`GetGUC<Type>`/`SetGUC<Type>`) with per-session overlays over
   postmaster defaults (xtc: `xtc_cfg`).

5. **Signals as IPC.** 690 `signal`/`pqsignal`/`sigaction` sites.
   Signals target the process, not a thread, and async-signal-safety
   cripples handlers. Move to explicit messages/notifications
   (xtc: signals -> mailbox messages, `xtc_notify` as the latch).

6. **`proc_exit`/`exit`/death semantics.** 103 `proc_exit`, 557 other
   `exit`, 159 `PG_TRY`/`sigsetjmp`, 11319 `ereport`/`elog`. **The
   single most dangerous change:** under threads, one thread's crash
   or `exit()` kills the entire server. Every FATAL path and uncaught
   SIGSEGV must unwind ONE session, not the process. (See Part 3 / F5.)

7. **Memory contexts & palloc.** 3774 `palloc`/`pfree` sites; 739
   `CurrentMemoryContext`/`TopMemoryContext` refs. `CurrentMemoryContext`
   must become per-thread; the allocator underneath thread-aware. Call
   sites are preserved; the substrate changes (xtc: `xtc_mctx`).

8. **Shared memory becomes trivial -- and that is a trap.** With one
   address space, DSM is just the heap. Code relying on shmem being a
   separate fixed mapping, and on the postmaster reinitialising shmem
   after a crash, has subtly different semantics now.

9. **Extensions.** 309 backend files touch `fmgr`; the third-party
   ecosystem is vast and assumes process isolation + free use of
   globals. Needs predictable bridging (see "Extension bridging").

10. **Tooling debt.** None of this is maintainable without CI that
    hard-fails on a new bare `static`, raw `signal()`, or unsafe libc
    call, plus a TSan build. The tooling is itself a deliverable (F1).

---

## Part 2 -- Foundational changes (the substrate, in order)

Non-negotiable prerequisites. Nothing risky is safe until these exist.

- **F1. Classification harness, in CI, before any conversion.**
  Annotation macros + clang lifetime tools (`pgguclifetimes`,
  `pg_static_vars`) + `s_globals`/`s_signals`/`s_libc` lints + TSan +
  a ratcheting baseline. See `F1_CLASSIFICATION_HARNESS.md`.

- **F2. Thread/OS abstraction layer.** `port/pg_threads.h` over xtc L0
  (`os_thread`, `os_mutex`, `os_tls`, `os_locale`, `os_getopt`,
  `os_strerror`, `os_env`). One seam for threads + every unsafe-libc
  wrapper.

- **F3. Dual-mode switch.** `IsMultiThreaded` + `multithreaded` GUC
  (`PGC_POSTMASTER`), so one binary runs both models and every change
  is independently shippable. (Heikki has this.)

- **F4. Single `Session` struct.** The endgame for issue 1 is not
  thousands of `__thread` vars but ONE struct owning all per-session
  state, reached via one `session_local` pointer. xtc's `xtc_proc`
  *is* this container. Migrate cohesive global clusters into it, one
  subsystem at a time.

- **F5. Crash-containment boundary.** Per-thread SIGSEGV ->
  `xtc_exit_self`; `proc_exit` chain -> `xtc_proc` cleanup; a
  supervisor observes the death. The bridge to "let it crash." Until
  F5 exists, threaded mode is a debugging toy. **This is now REAL, not
  modelled:** xtc R1 landed the containment primitive
  (`xtc_fault_guard_install` installs a per-thread sigaltstack handler
  for SIGSEGV/BUS/FPE/ILL; `xtc_proc_recovery_arm` is a sigsetjmp-
  shaped recovery frame the handler longjmps to). A genuine wild-
  pointer SIGSEGV in one session unwinds only that fiber's call stack;
  siblings survive. **Safety rule:** containment applies OUTSIDE
  critical sections only -- a torn WAL buffer (inside
  `START_CRIT_SECTION`) MUST still PANIC the process; xtc enforces this
  with `xtc_proc_critical_enter/leave`, which make a fault ESCALATE to
  process abort while crit-depth > 0 (the runtime mirror of
  ERROR-becomes-PANIC). **Held resources are released via exit-path
  callbacks (xtc R1, landed):** containment unwinds the CALL STACK
  only, so `xtc_proc_at_exit()` registers the teardown the runtime runs
  on BOTH a normal exit and a contained-fault recovery (before monitors
  observe DOWN) -- the adapter registers `xtc_lock_release_all` there
  (runtime-guaranteed: no lock-manager lock outlives a faulted session
  to wedge a peer) and hangs the backend's `TopMemoryContext` off
  `xtc_proc_mctx()` as a backstop. PG's own `AbortTransaction` /
  ResourceOwner / smgr / catcache cleanup stays PG-side, registered the
  same way. A monitor reads the (packed) DOWN signal with
  `xtc_down_decode()`, never a hand-rolled struct.
  Proof of concept (real SIGSEGV, contained, at_exit hook fires on both
  paths, sibling survives): `spikes/f5_crash_containment.c`.

- **F6. Runtime seam (`pg_xtc_glue.h`).** One PG-tree header mapping PG
  runtime APIs onto xtc, keeping xtc symbols out of PG core headers.
  Locks first (`xtc_lwlock`/`xtc_lrlock`/`xtc_lockmgr` are already
  PG-API-shaped), then latch (`xtc_notify`), then memory (`xtc_mctx`).

- **F7. Extension boundary (L5).** `PG_THREADSAFE_EXTENSION` +
  control-file `threadsafe` flag + `extension_thread_safety` policy
  GUC + the satellite-process bridge. Must exist before threaded mode
  ships to users, because *default-to-fork* is what makes the
  transition non-breaking. Proof of concept:
  `spikes/f7_satellite_bridge.c`. See "Extension bridging" below.

---

## Part 3 -- The broad transformation (beyond thread-per-connection)

Thread-per-connection is the conservative on-ramp. Fully exploiting
xtc + "let it crash" changes the architecture's shape:

- **V1. Supervision trees.** Replace the `fork()`-and-`waitpid` loop
  with an `xtc_supervisor` tree: root = server; children = subsystem
  supervisors (acceptor, WAL writer, checkpointer, autovacuum,
  bgworkers); each backend a supervised `xtc_proc` (transient policy:
  don't restart a normal client session; do restart infrastructure).
  A session bug unwinds itself; neighbours are untouched. Blast radius
  shrinks from "cluster restart" to "one session."

- **V2. Shared-nothing thread-per-core reactors.** Not N threads for N
  connections, but a fixed pool of one event-loop per core, with
  thousands of sessions multiplexed as `xtc_proc`s. Sessions become
  cheap (~1 KB mailbox vs ~10 MB fork). Connection count decouples
  from thread count. Tail latency becomes a budgeted property
  (`xtc_res`), not an accident. A "backend" goes from "an OS thread"
  to "an async actor with a mailbox" -- which is what a SQL session is.

- **V3. Mailboxes + selective receive.** The 690 signal sites collapse
  into typed messages: cancellation, SIGHUP reload, query-cancel,
  inter-backend signalling all become inspectable mailbox messages.

- **V4. `gen_server`-shaped services.** autovacuum, stats, logical
  apply workers become uniform call/cast/info servers with supervised
  restart and bounded mailboxes. `RegisterBackgroundWorker` ->
  `xtc_proc_spawn` under a supervisor.

- **V5. One async I/O substrate.** PG's aio and the rest unify on
  `xtc_io` (io_uring/epoll/kqueue). The reactor IS the wait loop.

- **V6. RCU / wait-free read-mostly caches.** relcache/syscache reads
  become wait-free via `xtc_rcu`/`xtc_lrlock`. This is where threaded
  PG *beats* the process model rather than merely matching it: shared
  caches were expensive across processes; now they are cheap and
  lock-free on the read path.

- **V7. Built-in resource governance.** `xtc_res` gives hard,
  backpressured caps on memory/fds/in-flight/bandwidth with high-water
  callbacks. Per-tenant budgets and graceful degradation instead of
  OOM-killer roulette -- a genuinely new capability.

### How the two halves connect

The conservative plan and the radical vision are the same path at two
speeds:

- F4 (Session struct) -> V2 (sessions as actors): once per-session
  state is one container, a session no longer needs to be a thread.
- F5 (crash containment) -> V1 (supervision trees): once a session can
  die alone, a supervisor can manage many.
- F2/F6 (OS + glue seams) -> V3/V5 (mailboxes, async io): once signals
  and io route through xtc, the reactor model is available.

**Guidance: build F1-F7 so they do NOT bake in "one thread per
session."** Make a session an object, not a thread; make death
containable; route waits through xtc. Then thread-per-connection is
just the first, safest *configuration* of an architecture that later
becomes a thread-per-core reactor without a second rewrite.

---

## Extension bridging -- bring the whole ecosystem along, predictably

The third-party ecosystem cannot be fixed by us; the migration must be
predictable and automatic, not a flag day.

**Rule:** every extension that loads today keeps working tomorrow,
classified at load time into one of two tiers:

- **`xtc-ready`** -- runs in-thread, shares the address space.
- **`fork-required`** -- runs in a supervised satellite process; the
  in-thread session proxies `fmgr` calls to it over an xtc channel.

**Default = `fork-required`.** An un-recompiled, un-annotated binary
`.so` (the entire existing ecosystem on day one) automatically runs in
a satellite, so its process-model assumptions stay valid. Nothing
breaks; nothing silently corrupts. This is the load-bearing decision.

**Promotion to in-thread is opt-in AND verified:** the author declares
`PG_THREADSAFE_EXTENSION()` *and* the extension passes the same F1
classification lint (packaged for out-of-tree builds). A declaration
that fails the lint is safely demoted to a satellite, not trusted.

| Extension state today | Class | Runs | Author effort |
|---|---|---|---|
| Recompiled + annotated + lint-clean | xtc-ready | in-thread | declare macro, fix flagged globals |
| Recompiled, declares, fails lint | demoted | satellite | must fix to be promoted |
| Un-recompiled / binary-only | fork-required | satellite | **none** |
| DBA-trusted binary | fork-required (or forced) | satellite | DBA sets control flag |

**The bridge:** a `fork-required` extension loads into a supervised
`xtc_proc`-hosted satellite *process* (real OS process -- coroutines
share an address space and give no isolation). `fmgr` calls marshal
args over the channel, run in the satellite, marshal results back; SQL
behaviour is identical. A crashing extension takes down only its
satellite (supervisor restarts it); the session gets a clean ERROR and
the server stays up. Shared-state extensions
(`shmem_request_hook`/`ShmemAlloc`, e.g. `pg_stat_statements`) attach
the satellite to the server's DSM region so shared counters survive.

The bridge is PG-side glue (`pg_xtc_glue.h`); xtc only supplies the
generic channel + supervised process host, knowing nothing about
`fmgr` or extensions (boundary preserved). This reinforces V1:
satellites are just more supervised children.

---

## Sequencing (minimal churn)

1. **Phase 0:** rebase Heikki's `heikki-threading` onto current
   `master` in the `xtc` branch; vendor xtc as the single-file
   **amalgamation** (`xtc.h` + `xtc.c`, generated by
   `dist/mkamalgamation.py`) dropped into the PG tree -- likely as a
   submodule at `contrib/libxtc` with `-DXTC_RELATIVE_LOC` for `#line`
   remapping to the original sources in debug builds -- NOT a separate
   `libxtc.a` linked via `--with-xtc`. The amalgamation auto-selects
   the epoll backend and needs only `-pthread -ldl -lm`. Land F1 + F2
   + F3 tooling. `multithreaded=off` default.
2. **Phase 1 (locks):** back `LWLock`/`LockManager` with xtc behind
   unchanged APIs (already API-compatible; lowest risk).
3. **Phase 2 (latch/wait):** `WaitLatch`/`WaitEventSet` -> `xtc_notify`
   + `xtc_io_poll`; pin each backend to one loop. **Pinning is already
   guaranteed by xtc (R5 confirmed):** a proc is bound to its spawn
   loop and is never work-stolen (via `xtc_async` -> pinned task ->
   owner-only FIFO), so a backend's session-local `__thread` state
   stays on one OS thread without any opt-in flag.
4. **Phase 3 (memory):** `MemoryContext` -> `xtc_mctx` shim (most
   pervasive; do deliberately).
5. **Phase 4 (process/supervision):** postmaster -> `xtc_app` +
   `xtc_supervisor`; backends -> `xtc_proc`; F5 + F7 land here.
   WAL writer/checkpointer/autovacuum stay separate processes; DSM
   stays PG-owned.
6. **Phase 5 (aio/guc/obs):** aio -> `xtc_io`; GUC -> `xtc_cfg`; wait
   events/`pg_stat_*` -> `xtc_log`. Threaded mode TSan-gating from here.
7. **Phase 6 (RCU, stretch):** relcache/syscache -> `xtc_rcu`. The
   "fully embrace xtc" endpoint; where threaded PG starts winning.

## Open risks

- The Phase 0 rebase (Heikki's branch is ~1.5 yr behind) is the real
  early cost.
- Signal routing under threads -- needs a spike before Phase 2.
- **Held-resource leakage on a contained fault.** xtc R1 containment
  unwinds the call stack only; resources are released by
  `xtc_proc_at_exit()` callbacks the runtime runs on both normal and
  contained-fault exit (PG's `AbortTransaction` + `LWLockReleaseAll`
  re-homed there, plus `xtc_lock_release_all` so no lock-manager lock
  outlives a faulted session). `xtc_proc_mctx()` reclaims allocations
  as a backstop. A missed registration does not crash but can wedge
  peers; this is now a wiring task, not a missing primitive.
- **Runaway non-yielding backend.** In a cooperative coroutine model a
  backend that spins without reaching a yield point cannot be
  preempted. Cancellation is cooperative: hold an `xtc_abort_token`
  per backend, fire its `xtc_abort_source` from `statement_timeout`,
  and poll `xtc_abort_token_is_aborted()` at `CHECK_FOR_INTERRUPTS()`
  sites -- delivering a cancel at the next yield point. There is NO
  in-thread preemption (a fiber on a shared stack has no safe
  preemption point); the lever for truly un-cooperative work is the
  OS-process tier -- the fork-required satellite, a real process that
  can be signalled/killed. (xtc's planned `XTC_YIELD_CHECK` watchdog
  will flag over-budget procs as telemetry, with a queryable
  threshold, and can be bridged to fire the abort token.) Doc rule:
  in-thread = cooperative (place yield points); un-cooperative work =
  fork-required tier.
- xtc is pre-1.0, but the lock ABI for the PG adapter is now frozen:\n  `xtc_lwlock_t` / `xtc_lrlock_t` / `xtc_lockmgr_t` (+ option/stats\n  structs and entry points) are SemVer-stable as of xtc 0.4.0\n  (`docs/abi-stability.md`), layout changes via the deprecation cycle\n  only. Still freeze the `pg_xtc_glue.h` surface early on our side.
- Windows SEH-based containment is deferred upstream (the fault guard
  is a no-op there); threaded mode on Windows has no crash containment
  until that lands.
- Effort: ~13 person-weeks to a "1 process, N backends" prototype
  (M16 estimate), plus the rebase, plus >= a quarter to harden.
