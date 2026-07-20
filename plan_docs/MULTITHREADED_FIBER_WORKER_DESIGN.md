# Multithreaded Fiber-Worker Design

Design + investigation for transitioning PostgreSQL backend WORKERS from
`xtc_proc` (pinned, non-stealable) to work-stealable fibers, monitored via
libxtc supervision, with libxtc fiber crash containment.

**Status:** DESIGN ONLY. No conversion code landed in this pass. This
supersedes `/tmp/libxtc-migratable-proc-request.md` (no new libxtc API is
needed -- the migratable path already exists on the task/coro side).

libxtc reference: `/home/gburd/ws/xtc` v1.24.0 (pinned e944d00). All
`file:line` citations below are into that tree unless prefixed with `pg:`.

---

## 0. The load-bearing finding (verified from source AND empirically)

The user's premise -- "libxtc provides fiber crash containment and fiber
supervision" -- is TRUE, but the mechanism is **proc-keyed, not
coro-keyed**. Both containment and supervision are armed by the L3
`xtc_proc` layer and keyed on the thread-local `__current_proc`. A *pure*
`xtc_async`/`xtc_task` fiber that never enters `__proc_entry` gets NEITHER.

Proven with two throwaway probes (built against `build_unix/libxtc.a`,
v1.24.0, io_uring backend):

| Case | SIGSEGV outcome | `xtc_self()` | Cite |
|------|-----------------|--------------|------|
| pure `xtc_async` fiber | **process killed (exit 139)**, no DOWN | `is_none=1` (NOPROC) | probe `/tmp/probe_fault.c`, `/tmp/probe_fiber.c` |
| `xtc_proc` (spawn_monitor) | **contained -> DOWN kind=2 signal=11**, process survives | valid pid | probe `/tmp/probe_proc_fault.c` |

Root cause in source:

- The fault handler `__xtc_fault_handler` keys on `__current_proc` and
  requires `p != NULL && p->recovery_armed && p->crit_depth == 0` to
  contain; otherwise it re-raises with `SIG_DFL` and the process dies
  (`proc.c:2101-2137`). It reads `__xtc_current_coro` ONLY for
  stack-overflow guard-page detection (`proc.c:2109-2126`), never to
  contain.
- The recovery frame lives on the **proc struct** (`p->recovery_buf`,
  `p->recovery_armed`, `p->crit_depth`; `proc.c:373-383`) and is
  auto-armed in `__proc_entry` via `xtc_proc_recovery_arm()`
  (`proc.c:965,999-1006`).
- `__xtc_proc_recovery_slot()` (the arm-slot for the
  `xtc_proc_recovery_arm()` macro) returns a throwaway `&__recovery_dummy`
  when `__current_proc == NULL` (`proc.c:2256-2263`), so
  `xtc_proc_recovery_arm()` from a pure fiber is a **silent no-op**.
- `xtc_self()` is `__current_proc != NULL ? __current_proc->pid :
  XTC_PID_NONE` (`proc.c:1242-1245`).
- `xtc_launch`'s advertised "per-fiber recovery" (`xtc_launch.h:74`) is
  delivered by spawning an `xtc_proc` internally
  (`orc/launch.c:94` -> `xtc_proc_spawn`, `:102` -> `xtc_monitor`). "Per
  fiber" in libxtc's vocabulary = "the recovery frame armed on the proc's
  fiber," i.e. proc-hood.

**Consequence for this design:** we do NOT drop the proc wrapper to get
"pure fibers." A libxtc proc IS already a stackful coroutine
(`xtc_proc_spawn` -> `__proc_spawn_core` -> `xtc_async(loop,
__proc_entry, p, &t)`, `proc.c:1192`). The ONLY thing making a backend
worker non-stealable is that `xtc_async` hardcodes `pinned=1`
(`coro_fctx.c:387`: `__xtc_task_spawn_ex(loop, __xtc_coro_step, c, 1,
&t)`; also `coro_uctx.c:622`, `coro_winfiber.c:121`).

So "transition workers to pure fibers" is realized as: **keep the proc
identity + recovery + supervision, and make the proc's underlying coro
stealable (`pinned=0`).** That is one libxtc knob (a `pinned`/`migratable`
field, or a new spawn entry) plus the PG-side migration-safety work. It
gives migratability WITHOUT losing containment, supervision, identity, or
the mailbox -- all of which a genuinely pure fiber would forfeit and we
would have to rebuild.

This is the "defer with invariant" answer to the DECISION: the goal
(stealable workers) is met on the proc-backed coro; dropping to bare
`xtc_async` is rejected because it deletes the exact features the same task
tells us to preserve (containment + supervision + ExitPostmaster
escalation).

---

## 1. Exact libxtc APIs (with citations)

### 1.1 Spawn / migratability

- Backend workers today: `xtc_proc_spawn_monitor(loop, xtc_carrier_proc,
  entry_arg, &po, &bpid, &ref)` (`pg:pg_xtc_carrier.c:430`). This is the
  atomic spawn+monitor; the caller (the supervisor) must be a proc
  (`xtc_proc.h:126`).
- Under the hood: `xtc_proc_spawn` -> `__proc_spawn_core` ->
  `xtc_async(loop, __proc_entry, p, &t)` (`proc.c:1192`). `xtc_async`
  spawns the coro `pinned=1` (`coro_fctx.c:387`).
- Stealable path: `__xtc_loop_enqueue` only pushes to the Chase-Lev deque
  (steal-visible) when `!t->pinned` (`loop.c:310-313`). A pinned task
  always goes to the owner-only FIFO, never stolen.
- The stealable spawn entry already exists: `xtc_task_spawn` -> pinned=0
  (`task.c:191-194`), `xtc_exec_spawn` -> pinned=0 (`exec.c:876-880`),
  `xtc_exec_async` -> `xtc_async` (pinned=1) (`exec.c:894-899`).
- `xtc_proc_opts_t` has NO pinned/migratable field today
  (`xtc_proc.h:72-79`).

**The one libxtc dependency:** a way for the proc layer to spawn its coro
`pinned=0`. Cleanest shapes, in preference order:
1. `xtc_proc_opts_t.migratable` (opt-in; default 0 preserves today's
   behavior) threaded down `__proc_spawn_core` -> a `pinned` parameter to
   `xtc_async` (which today hardcodes 1). Smallest, ABI-safe (zeroed opts
   == old behavior).
2. A parallel `xtc_async_ex(loop, fn, arg, pinned, &t)` the proc layer
   calls with `pinned = !migratable`.

Both are internal to libxtc; PG only sets `po.migratable = true`. VERIFY:
this is the one item to confirm/request from libxtc -- everything else in
this design uses APIs that already exist.

### 1.2 Fault containment

- `xtc_fault_guard_install()` (`proc.c:2199`, decl `ptc_ext.h:24` /
  `xtc_proc.h`): installs the process-wide `SIGSEGV/SIGBUS/SIGFPE/SIGILL`
  handler on a per-thread `sigaltstack`. Idempotent; called once per loop
  thread. Already called by the carrier supervisor
  (`pg:pg_xtc_carrier.c:245`).
- Auto-armed recovery frame: `__proc_entry` arms it before the body
  (`proc.c:999`), so containment covers a fault ANYWHERE in the proc.
- Escalation preserved: a fault inside a critical section (`crit_depth >
  0`) or a stack overflow is NOT contained -- it re-raises `SIG_DFL`
  (`proc.c:2118,2128-2137`), i.e. PANIC/whole-process death.

Because we keep the proc wrapper, containment is **unchanged** for
migratable workers. This is the reason not to go bare-fiber.

### 1.3 Supervision / exit notification

- Monitor is established atomically at spawn: `xtc_proc_spawn_monitor`
  (`proc.c:1233`, via `__proc_spawn_core` rel==2, `proc.c:1160-1178`). No
  NOPROC race.
- On exit, `__notify_links_and_monitors` sends a `'D'` DOWN message
  carrying `{ref, pid, reason, exit_kind}` to each watcher
  (`proc.c:2836,2899-2907`). `exit_kind` is set in `__proc_entry`: 0
  clean, 1 app-exit, 2 signal (`proc.c:1004,1014,1017`).
- The watcher decodes with `xtc_down_decode_ex` into `xtc_down_info_t {kind,
  signal, exit_code, pid}` (`proc.c:2562`, header `xtc_proc.h`). Kinds:
  `XTC_DOWN_KIND_CLEAN/EXIT/SIGNAL/NOPROC` (`proc.c:2616-2635`).
- The supervisor is itself a proc that `xtc_recv()`s DOWN messages
  (`pg:pg_xtc_carrier.c:255-258`). This is the notification path today and
  it stays.

### 1.4 Identity

- `xtc_self()` -> pid, or NOPROC off a proc (`proc.c:1242`). Works inside a
  proc-backed worker regardless of migration (the proc keeps its identity
  across a steal; only its executing loop changes).
- `xtc_task_t *` handle: stable for the task's lifetime -- a stolen task is
  NOT recycled by the thief (`task.c:23-31`, `loop.c:480-483`), so the
  handle stays valid across migration.
- `__xtc_current_task()` -> the running coro's task (`coro_fctx.c` /
  `coro_int.h:98`).
- `xtc_proc_wake(pid)` (`xtc_proc.h:165`, `proc.c:1360-1367`): cross-thread
  loop nudge; a stale/gen-mismatched pid is a harmless no-op.
- `xtc_waker` (`task.c:196-262`): task-handle-based, explicitly
  cross-thread-safe wake (see the UAF note at `task.c:238-256`).

### 1.5 Await

- `xtc_await(task, &result)` (`xtc_async.h`, `coro_fctx.c:290`) recovers a
  coro's return value; from outside a coro it drives the loop. NOT used for
  worker supervision (we use the monitor/DOWN path, which classifies
  crash-vs-clean; await only returns a value and cannot distinguish a
  contained fault from a return).

---

## 2. Chosen supervisor shape + exit classification

**KEEP the proc supervisor watching proc-backed (migratable) fiber
workers. Change nothing about supervision.**

Rationale:
- `xtc_orc`/`xtc_svr` child specs take `xtc_proc_fn` and act on proc DOWN
  (`xtc_orc.h:52-58`, header comment `:1-16`). They CANNOT supervise a bare
  fiber. A bare-fiber worker would have no DOWN, so we would have to invent
  a completion callback + crash surface from scratch -- and there is no
  crash surface for a bare fiber (probe: it just kills the process).
- Workers stay proc-backed, so `xtc_proc_spawn_monitor` + the existing
  per-loop `xtc_carrier_supervisor_proc` (`pg:pg_xtc_carrier.c:227-460`)
  work unchanged. The supervisor is a proc; it does not need to migrate
  (it is long-lived and loop-local by design).

Exit-classification mapping (already implemented, preserved verbatim,
`pg:pg_xtc_carrier.c:289-405`):

| libxtc DOWN | source | PG action |
|-------------|--------|-----------|
| `KIND_CLEAN` | proc returned / `xtc_exit_self(0)` (`proc.c:1014`) | quiet log; postmaster reaps |
| `KIND_NOPROC` | monitor raced clean exit (`proc.c:2616`) | benign; postmaster reaps |
| `KIND_SIGNAL` | contained fault, `di.signal` = signal (`proc.c:1005`) | **genuine crash**: `g_xtc_genuine_crash=1` + `SetLatch(postmaster)` -> `ExitPostmaster` |
| `KIND_EXIT` | non-zero `xtc_exit_self(code)` (`proc.c:1017`) | postmaster reaps under restart policy; no escalation |

**Crash escalation is unchanged**: a genuine contained fault in a worker
still fans out DOWN(KIND_SIGNAL) -> supervisor sets `g_xtc_genuine_crash`
-> postmaster consumes via `xtc_pg_consume_genuine_crash()`
(`pg:pg_xtc_carrier.c:1160-1164`) -> `ExitPostmaster`. Migration does not
touch this: the DOWN is delivered to the watcher's mailbox regardless of
which loop the worker died on, because `__notify_links_and_monitors`
`xtc_send`s to the watcher pid (`proc.c:2899`), and `xtc_send` is
cross-loop by pid.

**One migration-visible subtlety to verify at the flip:** the supervisor
that watches a worker is the per-loop supervisor on the worker's *spawn*
loop. If the worker migrates to another loop and faults there, the DOWN
still routes to the original watcher (monitor is by pid, established at
spawn). The fault is contained on whichever loop it executes on (the fault
guard is installed on every loop thread, `pg:pg_xtc_carrier.c:245`). So
containment + escalation survive migration -- no change needed, but this
is a test target (see Phase F).

---

## 3. Identity re-keying + fiber-ctx-hook redesign

### 3.1 Identity: mostly UNCHANGED (this is the payoff of staying proc-backed)

Because workers keep proc identity, `xtc_self()` still returns a valid pid
inside a migratable worker, and the two proc-pid-keyed wake paths keep
working:
- The latch owner-fiber capture at the park point
  (`pg:waiteventset.c:1471-1479`): `set->latch->owner_fiber_{loop,local,gen}
  = xtc_self()`.
- The cross-thread SetLatch fiber wake in PG's lmgr proc.c: snapshot
  `owner` pid under the spinlock, then `write(sem_wake_fd)` + secondary
  `xtc_proc_wake(owner)` (`pg:lmgr/proc.c:2465-2509`).

**Critical property that makes migration safe here:** the fd write is the
**load-bearing wake** (level-triggered readiness = stored signal); the
epoll fd the fiber parks on becomes readable and the loop's io dispatch
resumes the task. `xtc_proc_wake(pid)` is only a *secondary nudge* to pop
the target loop out of poll (`pg:lmgr/proc.c:2500-2509`, and the
`xtc_proc.h:158-165` contract: "delivers no message; woken proc just
re-evaluates; a spurious wake is always safe"). A stale/gen-mismatched pid
after migration is a harmless no-op.

But there IS a migration gap: after a worker migrates from loop A to loop
B, its pid's `loop_id` is unchanged (pid is assigned at spawn,
`proc.c:1136-1139`), but the proc now parks on loop B. `xtc_proc_wake(pid)`
resolves the target loop from the pid's `loop_id` (`proc.c:1360-1367`) ==
loop A -- it would nudge the WRONG loop. Two mitigations, either
sufficient:
1. Rely on the fd write (load-bearing) + loop B's own io poll waking the
   fiber -- the nudge is best-effort and its miss only costs latency, not
   correctness (the fd is already readable; loop B will observe it on its
   next poll iteration). This is likely fine given the existing
   "secondary nudge" contract, but must be measured (park latency under
   migration).
2. Re-capture the owner identity on EVERY park from the fiber's *current*
   loop, not the spawn loop -- i.e. store a loop-current handle at the park
   point (`waiteventset.c` already captures at the park point,
   `:1463-1466`), and use a wake primitive that targets the current loop.
   The task-handle `xtc_waker` (`task.c:196`) is loop-accurate
   (`out->loop = task->loop`) and cross-thread-safe, but note `task->loop`
   is the HOME loop, not the running loop (steal does not rewrite
   `t->loop`; `loop.c:465-469`). So the robust primitive is
   `xtc_proc_wake` re-derived, OR a small libxtc addition to wake by the
   loop the proc is *currently parked on*.

**Decision:** at the flip, keep the fd write as the correctness guarantee
(it already is) and treat `xtc_proc_wake` as best-effort; add the
loop-accurate nudge only if park-latency measurement shows a regression.
This keeps identity re-keying to ZERO code changes for the common path and
isolates the one migration-specific wake question to a measured decision.
(Open question O-3.)

### 3.2 Fiber-ctx-hook redesign: carrier re-resolution goes LIVE

The lazy fiber-ctx hook (`pg:pg_xtc_carrier.c:637-707`) already does the
right thing structurally; ONE ponytail-marked assumption becomes live at
migration:

Today the restore trusts the SAVED carrier root (`pg:pg_xtc_carrier.c:695`
ponytail note): "pinned means the saved carrier == executing loop's
carrier, so no loop->carrier re-resolution is needed yet. When fibers can
migrate ... the carrier must be re-resolved from the loop this fiber
actually resumed on (`__xtc_current_loop`)."

At the flip, `xtc_pg_fiber_ctx_restore` must:
1. Chain the proc-layer restore (reinstall `__current_proc`) -- unchanged
   (`:680-681`).
2. `PgRuntimeRestoreCurrentWorkLazy(&fc->snap)` for the 5 non-carrier roots
   -- unchanged.
3. **NEW:** re-resolve the `carrier` root from `__xtc_current_loop` (the
   loop the fiber resumed on), NOT the saved value. libxtc exposes the
   current loop only via the internal `__xtc_current_loop`
   (`loop_int.h:266`); PG already declares such internal externs
   (`pg:pg_xtc_carrier.c:536-537` does this for the ctx hooks). Map
   loop -> PG carrier via the existing per-loop carrier array
   (`g_xtc_exec` / `xtc_exec_loop`, used at `pg:pg_xtc_carrier.c:243`).

This is the SINGLE correctness change the hook needs to make migration
safe, and it is exactly the ponytail note's upgrade path. The 5 derived
roots and the 230 session-GUC pointers re-derive lazily via owner tokens
(unchanged; the measured 5ns lazy path).

The proc-pid owner-guard in the hook (`xtc_pg_fiber_ctx_is_current`,
`:598-604`, `xtc_pid_eq(fc->owner, xtc_self())`) stays valid because the
worker keeps its proc pid across migration. No re-keying to `xtc_task_t*`
is needed -- another payoff of staying proc-backed. (A bare fiber would
force this rewrite; we avoid it.)

---

## 4. No-steal guard design + resume-on-same-loop, RESOLVED

### 4.1 The resume-on-same-loop question -- ANSWERED from source

**A running (non-parked) fiber is NEVER moved mid-instruction.** Stealing
operates ONLY on the Chase-Lev deque, which holds tasks that are
SCHEDULED-but-not-running:
- A task is pushed to the steal-visible deque only in `__xtc_loop_enqueue`,
  and only when `!pinned` (`loop.c:308-313`). Enqueue happens at spawn and
  at wake-from-park (`xtc_waker_wake`: PARKED -> SCHEDULED -> enqueue,
  `task.c:230-234`).
- A thief pops from the victim's deque via `xtc_deque_steal`
  (`exec.c:104,115`), then enqueues locally and runs it on ITS loop
  (`loop.c:597-602`, `loop.c:674-679`).
- The owner pops a task off the deque to RUN it (`__queue_pop` ->
  `xtc_deque_pop`, `loop.c:337`); once running, it is not on any deque, so
  no thief can take it. It runs to its next yield/park entirely on the
  loop that popped it.

**Therefore the hazard is NOT "stolen mid-instruction."** The only hazard
is: **a fiber PARKS while holding OS-thread-affine state, and RESUMES on a
different loop** (a thief woke+stole it). Between park and resume the fiber
holds nothing on the CPU -- but any state it *captured that is bound to the
original OS thread* is now wrong.

This precisely defines the guard requirement: **prevent a fiber from
PARKING (reaching a yield point) while it holds thread-affine state that
would be wrong on another carrier.** A section that holds such state but
contains NO yield point is automatically safe -- the fiber cannot be stolen
inside it (it never re-enters a deque).

### 4.2 The no-steal primitive

libxtc's scoped primitives (verified present):
- `__xtc_unsafe_enter/leave/depth` (`preempt_int.h:38-46`): a per-thread
  nesting counter that defers involuntary SIGVTALRM preemption and guards
  the fault handler against unwinding a corrupt allocator arena. This
  guards INVOLUNTARY preemption, not voluntary park+steal.
- `xtc_proc_critical_enter/leave` (`proc.c:2290-2302`): increments
  `p->crit_depth`; a fault inside escalates instead of being contained
  (`proc.c:2118`), and the preemption handler defers
  (`__xtc_proc_crit_depth`, `proc.c:2315`). Again: preemption + fault
  escalation, NOT park+steal.

**Neither primitive prevents a voluntary park from resuming on another
loop** -- because, per 4.1, the design does not need that: a fiber inside
either bracket that does NOT yield cannot be stolen at all. The guard we
need is simply: **do not yield/park while holding thread-affine state.**

There are two ways to honor that, and the audit already established the
sites contain no yield point:
- **Assertion/tripwire (preferred):** wrap the affine sections in a
  no-steal bracket that, in assert builds under `USE_XTC_CARRIER`, verifies
  no yield occurred inside (a park inside a no-steal bracket is a bug). The
  existing pattern is `xtc_pg_backend_fiber_is_migratable()`
  (`pg:pg_xtc_carrier.c:1177-1181`) used as a tripwire (the ssl_sni
  invariant, plan_docs history). Reuse it: assert that no park happens
  while the affine bracket depth > 0.
- **Belt-and-suspenders:** if a site MIGHT yield (none identified do), use
  `xtc_proc_critical_enter/leave` so a park is at least not stolen -- but
  that requires a libxtc guarantee that a `crit_depth>0` proc resumes on
  the same loop, which does NOT exist today (crit_depth only affects
  preemption + fault escalation, `proc.c:2118,2315`). So this path needs a
  libxtc addition and is only pursued if a real yielding-affine-section is
  found. (Open question O-1.)

### 4.3 The guard sites (from the audit, `plan_docs/MULTITHREADED_PLAN.md`
"Fiber-ctx-hook AUDIT" + "Post-hook sequence")

Each site holds OS-thread-affine state; each was audited to contain NO
yield point, so each is safe-by-construction and gets a tripwire, not a
real park-pin:

1. **Raw spinlock (`slock_t` / `s_lock`)**: a spinlock section has no
   yield point (PG spinlocks forbid any CHECK_FOR_INTERRUPTS / palloc /
   syscall). A fiber holding one is running, not parked -> not stealable
   (4.1). Tripwire: assert no park while a raw spinlock is held under the
   carrier. Confirmed safe-by-construction; free.
2. **OpenSSL per-OS-thread error queue** (`ERR_clear_error()` ...
   `ERR_get_error()` span in `be-secure-openssl.c`): the span contains no
   yield point (the SSL calls are non-blocking BIO ops; the yield is at
   `WaitEventSetWait`, OUTSIDE the span -- audit finding). The error queue
   is thread-local in OpenSSL, so if a fiber parked mid-span and resumed on
   another OS thread it would read a foreign queue. Since it does not park
   mid-span, safe. Tripwire + (later) the `xtc_tls_*` swap dissolves the
   hazard entirely (that is a separate, already-staged change).
3. **`sigprocmask(UnBlockSig)` error-recovery window** (`postgres.c`
   ~5493/~7065): a brief window where the fiber's OS-thread signal mask is
   temporarily changed. No yield point inside. Tripwire.
4. **`unpack_sql_state` static buffer / other static scratch**: per-call
   static buffers assume single-threaded reentrancy on one OS thread. No
   yield inside the fill+use. Tripwire, or make per-fiber if a caller is
   found to yield.
5. **errno across a yield**: audited CLEAN (plan history) -- every
   backend-fiber yield site reads errno from a fresh syscall after resume,
   or sets it from a libxtc converted return code. No guard needed; the
   errno-anti-pattern sweep is the guard (a lint/audit, not runtime code).

**Design of the guard:** a single scoped macro pair
`XtcPgNoSteal{Enter,Leave}()` that, under `USE_XTC_CARRIER` + assertions,
bumps a per-fiber depth and asserts depth==0 at every park boundary
(`xtc_pg_wait_fd` and the fiber-ctx save hook are the choke points). In
non-assert / process / non-fiber-threaded builds it compiles to nothing
(byte-for-byte preserved). This is a tripwire that makes "a wrong
assumption about a yield-free affine section" fail loudly the instant it
becomes false, rather than corrupting silently -- matching the existing
`ssl_sni` invariant style.

---

## 5. Carrier + crash-escalation proc-dependency map

Every proc dependency in the worker path, and its migratable-worker
disposition (grep of `XTC_DOWN_KIND_*`, `xtc_proc_*`, `xtc_self`,
`spawn_monitor` across the carrier/backend files):

| Dependency | Site | Disposition under migratable workers |
|-----------|------|--------------------------------------|
| `xtc_proc_spawn_monitor` worker spawn | `pg:pg_xtc_carrier.c:430` | UNCHANGED except `po.migratable=true` |
| `xtc_fault_guard_install` per loop | `pg:pg_xtc_carrier.c:245` | UNCHANGED (guard on every loop; contains faults wherever the worker runs) |
| DOWN classify -> escalate | `pg:pg_xtc_carrier.c:289-405` | UNCHANGED (monitor is by pid; DOWN routes regardless of loop) |
| `g_xtc_genuine_crash` -> `ExitPostmaster` | `pg:pg_xtc_carrier.c:334-373,1160` | UNCHANGED (crash escalation preserved) |
| supervisor is a proc (`xtc_proc_spawn`) | `pg:pg_xtc_carrier.c:475` | UNCHANGED (supervisor stays pinned/proc; only WORKERS migrate) |
| fiber-ctx hook (`__xtc_fiber_ctx_*`, `xtc_self` owner-guard) | `pg:pg_xtc_carrier.c:537,598-707` | carrier root RE-RESOLVED from `__xtc_current_loop` (Section 3.2); owner-guard stays (proc pid survives migration) |
| latch owner-fiber capture (`xtc_self`) | `pg:waiteventset.c:1471-1479` | UNCHANGED (pid stays valid); nudge is best-effort (3.1) |
| fiber wake (`xtc_proc_wake(owner)`) | `pg:lmgr/proc.c:2465-2509` | fd write load-bearing (UNCHANGED); `xtc_proc_wake` best-effort, may miss after migration (O-3) |
| `xtc_pg_wait_fd` park | `pg:waiteventset.c:1483` (fd.c, method_xtc.c seams) | UNCHANGED as park; becomes the no-steal choke point (Section 4) |
| `xtc_in_backend_fiber` (raw `__thread`) | carrier + waiteventset | legitimately per-carrier; audited (plan history); UNCHANGED |

Nothing in the escalation chain is broken by migration. The two
migration-sensitive spots are (a) the carrier re-resolution in the hook
(Section 3.2, a required change) and (b) the best-effort wake nudge
(Section 3.1, a measured decision, not a correctness change).

---

## 6. Phased, incremental conversion plan

Each phase is independently gated: **two independent reviewers + build with
0 warnings + `test_backend_runtime` 0 Fail + process regress 245/245 +
`check-threaded`**. Process mode and non-fiber threaded mode are
byte-for-byte preserved throughout; fiber-worker changes are all under
`USE_XTC_CARRIER`. The tree is green after every phase.

**Phase A -- Carrier re-resolution in the fiber-ctx hook (STILL PINNED,
correctness-neutral).**
Wire `xtc_pg_fiber_ctx_restore` to re-resolve the `carrier` root from
`__xtc_current_loop` instead of the saved value (Section 3.2). While
pinned, resumed-loop == saved-loop, so this is a no-op-equivalent that
validates the mechanism. Delete the ponytail note. Gate: hook round-trip
test (`test_current_work_snapshot_lazy_restore`) + full green. This is the
smallest step and de-risks the one hook change before migration is live.
Validation targets: `check-runtime-lifecycles`, `check-global-lifetimes`.

**Phase B -- No-steal tripwire scaffolding (STILL PINNED, dead code).**
Add `XtcPgNoSteal{Enter,Leave}()` (assert-only under `USE_XTC_CARRIER`) and
the park-boundary assertion in `xtc_pg_wait_fd` + the fiber-ctx save hook.
Instrument the five affine sites (spinlock, OpenSSL ERR span, sigprocmask
window, static-scratch, errno-sweep-as-lint). While pinned the assertions
never fire (no migration), so this is dead scaffolding that documents and
enforces the invariants BEFORE they matter -- exactly the `ssl_sni`
tripwire pattern. Gate: full green + `check-threaded-world-core`.

**Phase C -- libxtc `migratable` opt-in (LIBXTC change; STILL DEFAULT
PINNED for PG).**
Add `xtc_proc_opts_t.migratable` (or `xtc_async_ex` with a `pinned` arg)
threaded to the coro spawn (Section 1.1). Default 0 == today's behavior, so
ALL existing spawns (PG's and libxtc's own) are byte-for-byte unchanged.
Bump the libxtc pin. PG sets nothing yet. Gate: libxtc's own tests + PG
full green (proves the ABI/default is neutral). This is the ONE libxtc
dependency; verify-before-request already done -- the migratable path
exists, only the proc->coro `pinned` threading is missing.

**Phase D -- FLIP: workers spawn migratable (MIGRATION GOES LIVE).**
Set `po.migratable = true` in the worker spawn
(`pg:pg_xtc_carrier.c:430`) and flip
`xtc_pg_backend_fiber_is_migratable()` (`pg:pg_xtc_carrier.c:1177`) to a
real per-fiber query. **Prerequisites that MUST be in place first: Phase A
(carrier re-resolution) and Phase B (no-steal tripwires).** This is the
step where a worker can first resume on a different carrier, so the hook's
carrier re-resolution and the affine tripwires become load-bearing. Keep
the supervisor pinned (it does not migrate). Gate: full green +
`check-threaded-workers` + `check-threaded-world-core` +
`check-runtime-lifecycles` + `check-global-lifetimes`, and a
fault-under-migration test (worker migrates to loop B, faults there,
supervisor still gets DOWN(SIGNAL), postmaster still `ExitPostmaster`).

**Phase E -- Wake-nudge accuracy (MEASURED, only if needed).**
Measure park latency under migration. If the best-effort `xtc_proc_wake`
miss after migration (Section 3.1 / O-3) shows a latency regression, add
the loop-accurate nudge (re-derive the current parked loop, or a small
libxtc "wake by current loop" addition). If no regression, this phase is a
no-op and the fd-write correctness guarantee stands alone. Gate: benchmark
delta + full green.

**Phase F -- Benchmark the win (USER-GATED big run).**
Re-run the A/B benchmarks. Expect the 54%-idle / `__xtc_exec_try_steal`
18.76% ceiling to move once runnable workers rebalance across idle
carriers. This is the payoff measurement; do NOT run it until D+E are
landed and green (per the plan's "no big re-benchmark until a gated change
warrants it").

**Which step first makes migration LIVE:** Phase D. What must precede it:
Phase A (carrier re-resolution) + Phase B (no-steal tripwires) + Phase C
(the libxtc `migratable` opt-in). A/B/C are all pinned/neutral and can land
in any order among themselves; D is the single flip.

---

## 7. Risks + open questions

- **O-1 (libxtc, only if a yielding affine section is found):** neither
  `__xtc_unsafe_enter/leave` nor `xtc_proc_critical_enter/leave` guarantees
  resume-on-same-loop across a VOLUNTARY park for a migratable proc
  (verified: they affect preemption + fault escalation only,
  `proc.c:2118,2315`). The audit found NO affine section that yields, so
  the tripwire design (Section 4) needs no such guarantee. IF a future
  affine-section-that-yields appears, request a libxtc "pin-for-section /
  no-steal-while-parked" bracket. VERIFY-BEFORE-REQUESTING: currently NOT
  needed.
- **O-2 (libxtc, Phase C):** the `xtc_proc_opts_t.migratable` opt-in (or
  `xtc_async_ex`). This is the one genuine libxtc dependency. It is a small
  internal threading of a `pinned` argument that `xtc_async` already
  hardcodes (`coro_fctx.c:387`); the stealable machinery
  (`task.c:191-194`, `loop.c:310`, `exec.c:104`) is fully built. Confirm
  the libxtc team's preferred shape.
- **O-3 (PG, Phase E, measured):** `xtc_proc_wake(pid)` resolves the target
  loop from the pid's spawn-time `loop_id` (`proc.c:1360-1367`); after
  migration the proc parks on a different loop, so the nudge may hit the
  wrong loop. Correctness is unaffected (the fd write is load-bearing and
  level-triggered; the parked fiber's actual loop wakes it on its own poll,
  `lmgr/proc.c:2500-2509` + `xtc_proc.h:158-165`), but park latency might
  regress. Decide with a benchmark, not speculation.
- **R-1:** supervisor/worker loop-affinity of DOWN routing under migration.
  Monitor is by pid (established at spawn); DOWN `xtc_send`s to the watcher
  pid cross-loop (`proc.c:2899`), so it routes correctly. Test in Phase D.
- **R-2:** the fiber-ctx hook's global-reset race (proc.c re-points
  `__xtc_fiber_ctx_*` on every spawn from any thread,
  `pg:pg_xtc_carrier.c:625-635`) is already handled (save returns the real
  proc; manual seams re-install roots). Migration does not worsen it, but
  re-verify the carrier re-resolution composes with the race in Phase A.

**No genuinely-missing libxtc feature.** Containment and supervision exist
(proven), and are retained by keeping workers proc-backed. The only libxtc
add is the `migratable` opt-in (O-2), and even that is a knob on existing
machinery, not a new subsystem.

---

## Appendix: probes (design-verification only, throwaway, in /tmp)

- `/tmp/probe_fiber.c`: pure `xtc_async` fiber -> `xtc_self()` NOPROC
  (`is_none=1`). Built + run against `build_unix/libxtc.a` (v1.24.0).
- `/tmp/probe_fault.c`: pure `xtc_async` fiber SIGSEGV -> process killed
  (exit 139), `xtc_proc_recovery_arm()` no-op. NO containment.
- `/tmp/probe_proc_fault.c`: `xtc_proc_spawn_monitor` child SIGSEGV ->
  contained -> supervisor gets `DOWN kind=2 signal=11`, process survives
  (exit 0). Containment IS proc-hood-dependent.
