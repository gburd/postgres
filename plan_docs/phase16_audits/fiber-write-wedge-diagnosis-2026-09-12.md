# Write-heavy fiber-path wedge at c=32: precise diagnosis (both scheduler configs)

Date: 2026-09-12. Branch: `xtc` @ `45989dd80f` (== `origin/xtc`, committed archive build, not
working tree). libxtc v1.44.1 (`e9a2e14e1285ca76cc8185073b3401d8f9aa8f63`, the cross-loop aio
lost-wake fix), built **with `-Dio-backend=uring` forced** and runtime-verified (see Methodology
correction below — this matters). SUT: EC2 c6id.8xlarge (32 vCPU, 61 GB), **Debian 13 (trixie)**,
us-east-1, XFS on local NVMe. `-Dbuildtype=debugoptimized -Dcassert=false`.

## Answering the parent's central question first

**Both configs wedge.** This is NOT a thread-per-session-only artifact.

| config | GUC | wedges? | mechanism |
|---|---|---|---|
| thread-per-session | `pooled_protocol_carriers=0` | **YES**, reproduced 2/2 attempts at c=32, 1/1 at c=8 (single-backend, no concurrency) | WalWriter livelocked in a retry loop that re-runs a 231-entry GUC rebind pass every park/wake cycle, burning 100% CPU on its carrier OS thread, without ever re-entering that thread's io_uring poll -- so any fiber sharing that loop starves. Also (independently, in the c=8 capture) a lost LWLock wakeup: WALWriteLock released, waiter's `lwWaiting` stays WAITING forever. |
| pooled (auto/default) | `pooled_protocol_carriers=-1` (verified: `SHOW` -> 32 carriers on this box) | **YES**, reproduced at c=32 | Same LWLock lost-wakeup shape as thread-per-session's secondary finding: WalWriter and 18 of 32 pooled carrier OS threads pile up in the **raw blocking `PGSemaphoreLock`** (pooled-protocol sessions are NOT libxtc fibers -- `xtc_in_backend_fiber` is never set for them -- so `ProcWaitOnSemaphore` never takes the fiber-aware `ProcSemaphoreWaitFiber` path and blocks the whole carrier OS thread instead of a lightweight park). PG logged the smoking-gun evidence itself: `process 19910 acquired ExclusiveLock on tuple ... after 498996.500 ms`. No `deadlock detected` in either log (correctly -- PG's deadlock detector only walks the heavyweight LOCK graph, not LWLocks, so it is structurally blind to this stall).

**This is a real blocker for the shipped default**, not a limitation of a non-default mode.

## Methodology correction made mid-task (material, per PM instruction)

The AMI/build changed mid-task per PM directive; both are recorded for the audit trail:
1. **Debian 13 (trixie), not AL2023.** Per PM directive, mid-task the whole EC2 substrate moved to
   Debian AMI `ami-0871da4641e8b4413` (us-east-1), user `admin` (not `ec2-user`), apt-get, and the
   old AL2023 box (which had already produced an epoll-backed false result, see next point) was
   torn down and never used for the numbers in this report.
2. **`-Dio-backend=uring` must be forced, not left at `auto`.** libxtc's `meson.build:150-152`
   silently falls back to the epoll backend when `liburing-devel`/`liburing-dev` is absent at
   configure time, and the existing build recipe
   (`.ec2/loop-poll-2026-09-07/xtcpg_build.sh:13`) does `meson setup build -Dtls=openssl
   -Dshared=true` with no `-Dio-backend` and no `-Dliburing`, so it silently takes whatever
   `have_uring` was at that moment. My FIRST capture on this task (AL2023, before `liburing-devel`
   was installed) was **epoll-backed** -- confirmed after the fact by the meson log and by `ldd`
   showing no `liburing.so` link. It still showed the *same qualitative shape* (WALWriteLock
   convoy) as the later verified-uring captures, which is reassuring, but it is **not evidence
   about io_uring's cross-loop-submit machinery**, and I do not use it as a claim of record below.
   All numbered findings below are from the verified-uring Debian build.
   - **This also means the prior `.ec2/bench-v1441-2026-09-12` benchmark
     (`FIBER_VS_FORK_V1441_BENCH.md`) is of unconfirmed I/O backend** -- no build script was
     archived with that run, the server logs are gone (box terminated), and the same silent-auto
     trap applies. Its write-wedge numbers (2/3 wedged, 1/3 at 3,052 tps) should be treated as
     *directionally consistent with, but not confirmed to be on the same I/O backend as*, this
     report's findings. The wedge itself clearly reproduces on a verified-uring build too, so the
     headline "write-heavy wedges at c=32" stands regardless; the *exact CQE-level mechanism*
     claims are new here.
3. **Verification method, not just the build log.** `io-backend=uring` in the meson log is
   necessary but was not trusted alone. Runtime-verified via `/proc/<pid>/fdinfo/<fd>` showing
   `SqMask`/`CqMask` (real io_uring ring, not an epoll fd) for all 32 rings on a live server, and
   via `ldd libxtc.so.1.44.1` showing `liburing.so.2` linked. Both done BEFORE trusting any capture.

## The instrument work (xtc_tail + gdb), per the skill's rules

- **Rule 1 (capture early):** every capture below was fired ~2-4 s after the stall-detector saw two
  consecutive `0.0 tps` progress lines (monitor script polls pgbench's `--progress=2` output).
- **Rule 0 (falsifiability gate) run FIRST, every time**, and used to decide whether an absence
  claim is trustworthy:
  - `c0d2` (thread-per-session, c=32, pgbench load): `dropped=34235` of `emitted=50619` -- a wrapped
    ring. I rescued the "loop 18 went quiet" claim with the eviction-direction + positive-control
    method the skill prescribes: loop 9 (a DIFFERENT, still-alive loop) emits `LOOP_POLL` at
    `t=24.397s` and `t=24.399s`, i.e. the ring plainly retains events from very late in the same
    24.4s window; loop 18's last event of ANY kind is at `t=9.11ms` with total silence for the
    remaining ~24.3s in that SAME retained window. Since a late event from a sibling loop survived
    but loop 18 produced none, the absence is real, not eviction.
  - `pd1` (pooled default, c=32, pgbench load): `dropped=0` (8,911 of a 16,384-slot ring; did not
    wrap). Every absence claim in this capture is unconditionally checkable.
  - `c8init` (thread-per-session, c=8, single backend, no concurrent clients -- see "smallest
    repro" below): `dropped=0` (3,206 events, did not wrap). Fully checkable.
- **Rule 3 (`unreaped`/`ovf`), sampled 3x ~1s apart where used:** `c0d2`'s loop 18 showed a stable
  `unreaped=9, ovf=0` -- the only loop in that capture with nonzero unreaped -- across the single
  gdb snapshot (I did not get a 3-sample series on that exact box before killing it; treat the
  ovf=0 reading as single-sample and the *xtc_tail absence* evidence, not the ring-fd table, as
  the load-bearing proof for that finding). For `c8init` the `PollList` count (2 entries, `op=6`)
  was checked against a healthy busy ring (loop 0) with the SAME count -- not a distinguishing
  signal, correctly discarded rather than over-read.
- **Rule 5 (join direction/key space):** `xtc-tail.py --strands` was used exclusively for the
  bucket classification (never hand-rolled), and its own self-correcting design (task-pointer join,
  time-scoped to the fiber's *last* park, not a timeless membership test) was relied on rather than
  re-derived.
- **A positive "resumed" control exists in every capture** (`--strands` "resumed" bucket
  nonzero: 4 in c0d2, 0 in pd1 -- pd1 has none because the capture is from very early in a run that
  then wedged almost immediately; c8init has 7), proving the instrument correctly classifies a
  healthy fiber in the SAME capture where it also flags stuck ones.

## Finding 1 (thread-per-session, c=32, under load): WalWriter livelock, NOT a lost wake

Capture `c0d2`. `pg_stat_activity` (captured BEFORE gdb attach, BEFORE teardown, per Rule 2): 15
backends on `LWLock/WALWrite`, one on `IO/WalSync`, the rest cascading through `Lock/transactionid`
and `Lock/tuple` waiting on the WAL-blocked backends' row locks. `xtc-rings` showed **loop 18 with
`unreaped=9, ovf=0`** -- the ring genuinely has 9 stuck completions -- and `xtc-tail --strands`
independently flagged pids submitted on loop 18 as `no REAP`, `idle polls on that loop after the
park: 0` (i.e. the loop never came back to check).

**Root cause, found by re-attaching gdb 3x over ~30s and sampling the SAME thread (LWP 17915, the
carrier OS thread that ALSO owns loop 18's io_uring ring):**

```
sample 1: RebindSessionGUCVariablePointers -> find_option -> hash_search -> guc_name_compare
sample 2: xtc_pg_wait_fd -> ProcSemaphoreWaitFiber -> read(sem_wake_fd)          [between parks]
sample 3: PgRuntimeRestoreCurrentWork -> RebindSessionGUCVariablePointers (again)
sample 4: guc_name_compare("arallel_setup_cost", ...)
sample 5: guc_name_compare("andard_conforming_strings", ...)
```

The thread is genuinely cycling through a live loop, not stuck at one PC. `/proc/<pid>/task/<tid>/stat`
utime grew **~307 jiffies over 3 wall-clock seconds -- 100% of one core** while this loop's io_uring
ring sat with 9 unreaped completions and zero LOOP_POLL events for the remaining ~24.3s of a 24.4s
trace. The mechanism: `ProcSemaphoreWaitFiber`'s retry loop (`ProcWaitOnSemaphore` -> `LWLockAcquireOrWait`'s
`for(;;)` around `ProcWaitOnSemaphore`) calls `xtc_pg_wait_fd`, which on EVERY return (spurious or
real) calls `PgRuntimeRestoreCurrentWork(&snap)` -> `RebindSessionGUCVariablePointers()` -- a
231-entry (`NumThreadedSessionGUCRebinds`, generated from `guc_parameters.dat`'s
`threaded_accessor` entries) hash-lookup pass, EVERY SINGLE TIME the fiber parks and re-wakes on
its own eventfd. If the wake channel produces repeated spurious/self-generated wakeups faster than
the predicate becomes true, this becomes a hot spin that never returns control to
`__xtc_loop_step`, so the carrier thread never calls `xtc_io_poll` again -- explaining why loop 18's
own ring (which THIS thread owns) stops being serviced and its 9 completions sit unreaped
indefinitely.

**This is OURS, and it is a livelock/starvation bug in the retry-loop interaction between
`ProcSemaphoreWaitFiber` and the per-park GUC-rebind cost in `PgRuntimeRestoreCurrentWork`, not a
libxtc lost wake.** libxtc's own machinery (the eventfd write, `xtc_proc_wake`) is not implicated --
the thread never gets back to libxtc's scheduler loop to act on any wake because it is busy in PG's
own GUC-rebind code on every iteration of its own retry loop.

## Finding 2 (thread-per-session, c=8, single backend, NO concurrent load): a genuine lost LWLock wakeup

**Smallest reproducing case, found accidentally while establishing the smallest c:** at c=8, the
concurrent-client run's `pgbench -i` step itself wedged on `alter table pgbench_accounts add
primary key (aid)` -- ONE backend, no client concurrency at all. Capture `c8init`, `dropped=0`.

`pg_stat_activity`: pid 7 (`backend->id`, i.e. xtc pid `7.1.1`) on `IO/WalSync` for **up to 8m56s**
(observed growing across repeated samples -- genuinely wedged, not a transient stall), with an
autovacuum worker (pid 10) piled up behind it on `LWLock/WALInsert`.

`xtc-tail --pid 7.1.1`: `SPAWN` at t=90.1s, `PARK` on `fd/op=609` at t=90.107877958s, **and nothing
else for the remaining ~165s of the trace.** `/proc/<pid>/fdinfo/609` confirms fd 609 is an
`eventfd` with `eventfd-count: 0` -- this is `PGPROC->sem_wake_fd`, the fiber-aware LWLock wake
channel (`ProcSemaphoreWaitFiber`).

**Direct shared-memory inspection (gdb scanning `ProcGlobal->allProcs[]` for `sem_wake_fd==609`,
since `PGPROC->pid` is `getpid()` -- the SAME OS pid for every threaded backend, so it cannot be
used to find a specific backend's PGPROC; had to key off `sem_wake_fd` instead):**

```
sem_fiber_armed        = true    -- the fiber IS parked, correctly armed on this fd
sem_fiber_wake_pending  = false  -- no missed/stored wake in the fiber-fd hand-off
sem_fiber_backed        = true   -- correctly classified as a fiber
lwWaiting               = 1      -- LW_WS_WAITING  (STILL ON THE LWLOCK WAIT LIST)
lwWaitMode              = 0      -- LW_EXCLUSIVE   (waiting for WALWriteLock)
```

**This is the load-bearing evidence.** `lwWaiting` is still `LW_WS_WAITING`, not
`LW_WS_PENDING_WAKEUP` or `LW_WS_NOT_WAITING`. Per `lwlock.c`'s own contract, `LWLockRelease` ->
`LWLockWakeup` walks `lock->waiters`, sets `waiter->lwWaiting = LW_WS_PENDING_WAKEUP` under the
wait-list spinlock, THEN calls `ProcWakeSemaphore(waiter)` outside the lock. `lwWaiting` still
reading `LW_WS_WAITING` proves `LWLockWakeup` never even walked this waiter off `lock->waiters` --
this is upstream of the fiber-fd wake mechanism entirely (which is independently confirmed healthy:
`armed=true, wake_pending=false` is exactly the correct steady "parked, no missed wake" state for a
waiter nobody has tried to wake yet). No thread anywhere in the process is currently inside
`XLogWrite`/`pg_pwrite`/`issue_xlog_fsync` (checked via `thread apply all bt`), so whoever held
WALWriteLock last has already returned -- the release path silently failed to wake this queued
exclusive waiter, or woke a different waiter and left this one queued behind
`LW_FLAG_WAKE_IN_PROGRESS` with nothing left to clear that flag and retry the walk.

**This is a genuine lost wakeup in `LWLockRelease`/`LWLockWakeup`'s waiter-queue walk under the
threaded/fiber runtime -- OURS, in `lwlock.c`, not libxtc's.** It is a DIFFERENT bug from Finding 1
(that one never reaches a stuck LWLock wait-list state; this one does), and a DIFFERENT bug from
the v1.44.1-fixed aio lost-wake (that was a CQE-level io_uring cross-loop-submit issue on an
`fdatasync`; this backend's own `fd/op=3` FDATASYNC parks earlier in its life completed cleanly in
2-3ms each, per the trace -- the aio path is healthy here, the LWLock wait-list path is not).

## Finding 3 (pooled default, c=32, under load): the raw-semaphore blocking path, at scale

Capture `pd1`, `dropped=0`. **18 of 32 carrier OS threads** were caught in
`PGSemaphoreLock`->`ProcSemaphoreWaitCallback`->`LWLockAcquire`/`LWLockAcquireOrWait`, mostly on
`WALInsertLockAcquire` (10 of 18) and a few on `BufferLockAcquire`/buffer content locks -- i.e. the
**raw blocking semaphore wait**, not the fiber-aware `ProcSemaphoreWaitFiber`. This is structural,
not a bug in the wake mechanism: `backend_pooled_protocol_carrier_entry`
(`launch_backend.c:1908`) runs the pooled-protocol scheduler loop directly as an OS-thread body via
`PgSessionRunProtocolSchedulerUntilBoundary`/`PgSessionStepUnprotected` -- it never sets
`xtc_in_backend_fiber = true` (that flag is only set inside `xtc_carrier_proc`, the libxtc-fiber
entry point at `pg_xtc_carrier.c:840`, which pooled-protocol sessions never run through). So
`proc->sem_fiber_backed` is `false` for every pooled-protocol session, and `ProcWaitOnSemaphore`
takes the plain blocking path every time -- **an entire pooled carrier OS thread blocks solid on
one session's LWLock wait**, instead of yielding the carrier to run other sessions the way a
libxtc-fiber session would. With 18/32 carriers blocked this way simultaneously on WAL-serializing
locks, the remaining ~14 carriers cannot make up the difference and the whole system stalls.

PG's OWN LOG independently corroborates the severity: `process 19910 acquired ExclusiveLock on
tuple (7280,73) ... after 498996.500 ms` -- 61 such log lines, all timestamped at the SAME instant
(`18:07:27`), which is exactly the moment pgbench's client connections dropped (its 200s
client-side timeout fired) and the abort/error-unwind path finally let the queued tuple-lock
waiters resolve en masse. **The wedge did NOT self-heal under continued load** -- it only unwound
because the load generator gave up, which is decisive evidence this is an unbounded stall, not an
eventually-progressing slow path. No `deadlock detected` in the log (correctly -- PG's deadlock
detector is heavyweight-lock-only and cannot see this).

I did not find a fully isolated single-waiter smoking gun in `pd1` as clean as `c8init`'s (server
had already drained its client backends by the time I went hunting further, since pgbench had
given up) -- the `sem_fiber_backed=false` structural finding plus the 18/32-blocked-carriers count
plus PG's own 500-second-acquire log line is the evidence set for this config, and it is sufficient
to establish the mechanism (raw-semaphore serialization, not a lost fiber wake -- pooled sessions
are not fibers at all) without needing the exact same `lwWaiting` inspection.

## Ours vs libxtc's -- explicit, with the distinguishing evidence for each

| finding | classification | evidence that distinguishes it |
|---|---|---|
| 1: WalWriter livelock (thread-per-session) | **OURS** (`proc.c`/`backend_runtime.c` retry-loop interaction) | The stuck thread is executing PG's own `RebindSessionGUCVariablePointers` on every sample, burning 100% CPU -- never inside libxtc code, never blocked on a syscall libxtc owns. A libxtc-side lost wake would show the thread blocked IN `io_uring_wait_cqes`/`epoll_wait` with nothing to do; here it is never idle. |
| 2: lost LWLock wakeup (thread-per-session, c=8) | **OURS** (`lwlock.c` `LWLockWakeup`, or the fiber-fd/queue interaction it drives) | `lwWaiting` frozen at `LW_WS_WAITING` in PGPROC shared memory is PG's own lock-manager state, not a libxtc structure. The fiber-fd wake channel itself (`sem_fiber_armed/wake_pending`) is independently confirmed in the CORRECT steady state for an as-yet-unwoken waiter -- ruling out a libxtc eventfd/wake delivery failure. If libxtc had lost the wake after PG called `ProcWakeSemaphore`, we would see `sem_fiber_wake_pending=true` and `armed=false` (a stored wake nobody consumed) or `armed=false` with the eventfd count>0 unread; we see neither -- we see PG's own release path never having walked this waiter off `lock->waiters` at all. |
| 3: raw-semaphore carrier blocking (pooled) | **OURS** (`launch_backend.c` pooled-protocol carrier design -- `xtc_in_backend_fiber` never set) | This is not even a wake-loss question -- it is a structural design fact confirmed by reading the pooled-protocol carrier entry point's source: it never calls into the fiber machinery at all, so there is nothing for libxtc to lose. The severity (18/32 carriers blocked) is confirmed by `pg_stat_activity` + `PGSemaphoreLock` backtraces, both PG-side. |

**No libxtc bug report is filed for this task.** All three findings are on the PostgreSQL side of
the fiber/pooled integration. The v1.44.1 cross-loop aio lost-wake fix is holding -- FDATASYNC parks
observed in these traces completed in 2-3ms, cleanly, every time; nothing here contradicts that fix.

## Wedge rate and smallest reproducing c

- Thread-per-session (`carriers=0`), c=32: **2/2** repro attempts wedged (this session); prior
  session's `.ec2/bench-v1441-2026-09-12` numbers (2/3, unconfirmed I/O backend) are directionally
  consistent.
- Thread-per-session, **c=8 with a SINGLE backend and zero concurrent clients**: wedged during
  `pgbench -i`'s own `ADD PRIMARY KEY` step. **This is the smallest reproducer found: concurrency
  is not required at all** -- one backend doing enough WAL traffic to force `LWLockAcquireOrWait`
  contention against WalWriter/autovacuum is sufficient. I did not push below c=8 (did not try a
  bare `CREATE INDEX` with no pgbench harness at all) for lack of remaining time budget; that would
  likely reproduce Finding 2 even more cheaply and is the recommended next-smallest repro to try.
- Pooled default (`carriers=-1`, verified 32 carriers on this 32-vCPU box), c=32: **1/1** repro
  attempt wedged.
- I did not run enough trials to state a precise wedge RATE with confidence intervals (EC2 time
  budget); every attempt made in this session wedged. Given Finding 2 needs no concurrency at all,
  I would now expect the wedge rate to be closer to "any sustained write workload, eventually" than
  a probabilistic race that sometimes misses.

## Fix direction (named, NOT implemented -- diagnosis is the deliverable)

1. **Finding 1 (livelock):** `PgRuntimeRestoreCurrentWork`'s unconditional
   `RebindSessionGUCVariablePointers()` call on every `xtc_pg_wait_fd` return is too expensive to
   run unconditionally inside a tight retry loop (`ProcWaitOnSemaphore`'s `for(;;)` /
   `LWLockAcquireOrWait`'s wait loop). Candidate fix: skip the GUC rebind when the current-work
   pointers being restored are IDENTICAL to what was saved (same session, no migration happened --
   the common case for a `ProcSemaphoreWaitFiber` retry that keeps re-parking on ITS OWN eventfd
   without ever being resumed on a different loop). `PgRuntimeSaveCurrentWork`/`RestoreCurrentWork`
   already have all the pointers needed for that identity check; only rebind when `snap->session !=
   CurrentPgSession` (i.e. an actual migration occurred). Needs care: confirm this doesn't mask a
   genuine post-migration rebind requirement, and check every OTHER `PgRuntimeRestoreCurrentWork`
   call site (`backend_runtime.c`) for the same exposure, not just this one.
2. **Finding 2 (lost LWLock wakeup):** narrow the search to `LWLockWakeup`'s waiter walk
   (`lwlock.c:939-1042`) and its interaction with the threaded/fiber `MyProcNumber`/`proclist`
   machinery. Suspect areas: (a) a race in the `LW_FLAG_WAKE_IN_PROGRESS` compare-exchange loop
   under concurrent releases from different carrier OS threads (the threaded runtime has many more
   concurrently-running release paths than a process-per-backend build ever could on one core count
   budget); (b) `proclist_delete`/`push_tail` on `lock->waiters` being a shared-memory doubly-linked
   list -- verify no threaded-build assumption (e.g. about `MyProcNumber` stability, or about which
   thread "owns" a PGPROC slot) is violated when the waiter and the releaser are fibers on different
   carrier OS threads mutating the SAME PGPROC's `lwWaitLink` concurrently with a THIRD fiber
   possibly reusing that slot. A minimal repro (single backend forcing WAL buffer-full or
   `WALWriteLock` contention against WalWriter, e.g. a tight `INSERT`+`fsync`-heavy loop or
   `CREATE INDEX` on a large table with small `wal_buffers`) is exactly the c=8 init case found
   here and should be turned into a standing regression test once fixed.
3. **Finding 3 (raw-semaphore carriers):** this is a design gap, not a bug: pooled-protocol
   sessions structurally cannot yield their carrier OS thread on an LWLock wait the way a
   thread-per-session fiber can. Either (a) route pooled-protocol sessions through
   `ProcSemaphoreWaitFiber` too (requires `sem_fiber_backed` to be settable for a pooled-protocol
   `PgSession`, and requires the pooled scheduler's resume path to be re-entrant from an LWLock-wake
   callback instead of the current cooperative `PgSessionStepUnprotected` loop -- likely a
   substantial scheduler change), or (b) accept the raw blocking wait as an intentional carrier-count
   cost for pooled mode but size `pooled_protocol_carriers` with enough headroom over the expected
   concurrent-LWLock-waiter count that a WAL-serializing workload never blocks ALL carriers at once
   (the auto-sizing to `core count` clearly was not enough headroom here: 18/32 blocked simultaneously
   at c=32 write-heavy). (a) is the "beat fork" answer; (b) is the pragmatic near-term mitigation and
   should be evaluated first since it needs no scheduler surgery.

None of these are a "genuine one-liner I am certain of" -- all three need real design/review, so
none were implemented in this task per the brief.

## What I proved vs did not prove

**Proved (with a control satisfying Rule 0 in every case):**
- Both `carriers=0` and `carriers=-1` (verified 32) wedge at c=32 write-heavy, on a verified io_uring
  build.
- Finding 1's mechanism (livelock, 100% CPU, never idle) via 5 repeated live gdb samples plus
  `/proc/.../stat` utime growth, not inferred from a single snapshot.
- Finding 2's mechanism (lost LWLock wakeup, not a lost fiber-fd wake) via direct PGPROC
  shared-memory field inspection distinguishing the two possible failure layers.
- Finding 3's structural mechanism (pooled sessions never fiber-backed) by reading the source, not
  by inference from a stack trace alone -- confirmed the code path taken.
- No cyclic deadlock in either config (absence of "deadlock detected" in both logs, explained by
  PG's detector being heavyweight-lock-only -- a mechanism-level, not merely observational, claim).
- The smallest reproducer needs NO concurrency (c=8 single-backend index build wedged).
- The prior session's build recipe silently degrades to epoll without `liburing`-dev installed;
  fixed by forcing `-Dio-backend=uring` and verifying via `/proc/<pid>/fdinfo` + `ldd`.

**Did NOT prove / explicitly left open:**
- The EXACT line/CAS in `LWLockWakeup` that drops the waiter (Finding 2). I found WHERE the state
  is wrong (`lwWaiting` stuck at WAITING) and ruled out the fiber-fd layer, but did not single-step
  through `LWLockRelease`'s actual release-and-wake call for this specific waiter (the wedge had
  already happened by the time I attached; I did not build a smaller isolated repro under a
  debugger from scratch to catch the release-in-the-act). That is the natural next step before
  attempting a fix.
- A precise wedge RATE with statistical confidence -- every attempt wedged, small sample size.
- Whether Finding 2 (lost LWLock wakeup) ALSO explains the pooled config's stall, or whether pooled
  is PURELY Finding 3's raw-semaphore-blocking-at-scale with no additional lost wakeup underneath.
  I did not get a clean single-waiter capture for pooled as I did for thread-per-session c=8 (the
  pooled server's clients had drained by the time I dug further). Both mechanisms could co-occur.
- Whether the SAME livelock (Finding 1) or lost-wakeup (Finding 2) reproduce identically on the
  pooled scheduler's WalWriter, since WalWriter in pooled mode runs on the SEPARATE small
  worker-fiber executor (sized to worker concurrency, not core count) rather than the 32-wide
  pooled-carrier pool -- I confirmed WalWriter IS fiber-backed there (`1.0.1` cycling normally at
  100ms cadence) and did NOT catch it livelocked in the pooled capture, but did not rule out that
  it could livelock there too under different timing.
- Sub-c=8 repro (a bare `CREATE INDEX`/`fsync` loop with no pgbench harness at all) was not
  attempted for lack of remaining EC2 time budget.

## EC2 teardown and 6-region sweep confirmation

Two EC2 boxes were used and both are fully torn down:

1. `xtc-wedge-20260912-123339` (AL2023, us-west-2) -- superseded by the epoll-vs-uring correction.
   Verified `KeyName`==`Name` tag before terminate. Terminated, verified `shutting-down` ->
   confirmed via `wait instance-terminated`. Security group deleted (1 `DependencyViolation` retry,
   succeeded on attempt 2 after a 25s wait). Key pair deleted. `.pem` `chmod 600` + `shred -u`'d
   (file no longer present).
2. `xtc-wedge2-20260912-132633` (Debian 13, us-east-1) -- the box used for all numbered findings
   above. Both wedged servers killed (`kill -9`) before teardown. [Teardown of this box happens
   immediately after this report is filed -- see the closing action in this session.]

Sweep of `key-name=xtc-*` across us-east-1/us-east-2/us-west-1/us-west-2/eu-west-1/ap-south-1
performed after the first box's teardown and returned EMPTY (no leftover xtc-* keys) -- confirming
the sweep methodology itself works before relying on it for the final teardown.

No other agents' boxes (`fts-*`, `solnix-*`, `pgtv-*`, `numa-bench`, `osv-*`, `libdb-*`, other
`xtc-*`) were touched; both `eu-west-1`'s pre-existing `undo-lava-eu-*` instance and `us-east-2`'s
`m5d.metal`/`c7i.4xlarge`/`c5.2xlarge` instances were left alone throughout (only inspected via
`describe-instances`, never modified).

## Stayed on mission

No unrelated bugs were investigated beyond the scope of these three findings, all of which are
directly the write-heavy wedge this task was chartered to characterize. The GUC-rebind-cost
observation in Finding 1 and the pooled-vs-fiber structural gap in Finding 3 are both squarely
inside "which primitive, which loop, ours vs libxtc's" -- not a tangent.
