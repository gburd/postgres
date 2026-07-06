# M16 xtc-carrier Findings -- threaded PostgreSQL on the xtc scheduler

Status: **(C) CONCURRENT WORKS ON A LOOP POOL** -- moving from a single carrier
loop to an N-loop executor pool (n_loops = CPU count) resolved the concurrent
lost-wakeup: N concurrent backends each run on a distinct loop, all return the
correct rows, and all exit cleanly (spawned == exited), and a new query after a
burst of concurrent ones is not wedged.  A second fix classifies the
fiber-backed backend as a pooled-logical PMChild (no pthread to join) so
`pg_ctl -m fast stop` completes instead of hanging in PM_WAIT_BACKENDS.  Built
via the Nix flake + meson/ninja (`-Dxtc=enabled`), rebased onto origin/master
(upstream + Sam's thread-per-session tree).

Prior status was **(B) PARTIAL** -- N SEQUENTIAL backends worked but N
CONCURRENT backends wedged the single loop: with two or more fibers parked at
once, exactly one resumed/exited and the rest were lost.  The single-loop model
was bringup scaffolding; the loop pool is the intended Phase-15 shape and
sidesteps the single-loop dispatch edge entirely, as predicted.

## Smoke validated 12/12 (2026-07-06, 8-loop pool on floki)

`scripts/xtc_smoke.sh` on a fresh HEAD install (btrfs-backed PGDATA) passes all
checks: select 1; pool sized to cores (8 loops); 6 concurrent backends with no
wedge; LISTEN/NOTIFY cross-fiber wakeup (item #3); ereport(ERROR) fiber unwind +
session recovery (item #4); clean fast stop; fiber accounting; io_method=xtc
data-file reads incl. multi-iovec integrity and no crash/corruption signature.

Fiber-accounting note: bg-worker fibers became fiber-eligible in `b2367b59c90`,
so a persistent worker (e.g. the autovacuum/logrep launcher) is spawned once and
stays alive.  The old smoke check required `spawned == exited`, which now fails
spuriously by the number of live workers (observed spawned=12 exited=11, the 1
surplus being the persistent worker).  The check now requires exited <= spawned,
spawned != 0, and the spawned-minus-exited surplus <= the count of live
"background worker launched as xtc fiber" lines -- a spawn-less exit (lost
bookkeeping) still fails.  This is a TEST fix; the runtime was correct.

## The two fixes (2026-07-04)

1. **Carrier loop pool** (`pg_xtc_carrier.c`): create the xtc app with
   `opts.n_loops = CPU count` (override `PG_XTC_CARRIER_LOOPS`), grab the
   executor with `xtc_app_exec()`, and place each backend fiber round-robin
   across `xtc_exec_loop(exec, i)` instead of a single `g_xtc_loop`.  Each loop
   runs on its own OS thread, so two concurrent backends never park on the same
   loop and cannot starve each other.
2. **Pooled-logical fiber reaping** (`launch_backend.c`): a fiber runs on a
   shared carrier loop, not a dedicated joinable pthread, so
   `PostmasterChildJoinThread()` -> `pg_thread_join()` on an unset handle hung
   shutdown.  Classify the fiber's PMChild as `PM_CHILD_CARRIER_POOLED_LOGICAL`
   and publish its exit via `PostmasterChildPublishPooledLogicalExit()`; the
   postmaster then reaps the slot through `process_pm_pooled_logical_exit()`
   (no join) and `PM_WAIT_BACKENDS` completes.

## Verified (2026-07-04, when the scratch data dir survived)

- 8-loop pool: 6 concurrent backends each `select pg_sleep(0.4); select 1;`
  all return rc=0, a following `select 42` runs (not wedged), and the fiber
  log shows `spawned == exited` with each backend on a distinct loop.
- single connection then `pg_ctl -m fast stop` completes cleanly
  (previously hung).
- with no client ever connected, fast stop was already clean; the hang was
  specifically the exited-fiber PMChild join.

## Validation note

The base tree's threaded TAP (`001_threaded_runtime.pl`) shells out to
`gmake -C contrib/hstore install`, which needs an autoconf/make-configured
tree; it does not run against a meson-only build in this environment (the
supported path is `gmake check-threaded`).  The pool + shutdown fixes are
validated by a full green meson build plus the direct runtime evidence above.

---

## Historical: single-loop status (superseded by the loop pool)

Status: **(B) PARTIAL** -- N SEQUENTIAL backends work perfectly (each spawns a
fresh xtc_proc fiber, returns the correct row, and EXITS cleanly; the loop slot
is reclaimed and reused with no leak).  N CONCURRENT backends return the
correct rows but do NOT tear down cleanly: with two or more backend fibers
simultaneously parked on the single carrier loop, exactly ONE resumes/exits and
the rest stay parked, wedging the loop (a lost-wakeup limitation).  The
original single-backend `select 1` milestone (below) still holds; the
fiber-teardown + thread-reset commits fixed the 2nd-backend GUC/timezone
NULL-context crashes for the sequential case.  OUT-OF-TREE, throwaway, on
branch `xtc-carrier` in `/home/gburd/src/multithreaded-postgres`.  Nothing
here touches the libxtc repo.

Verified 2026-07-04 with a fresh cluster, `multithreaded=on`, single xtc
carrier loop.  Always stopped with an explicit shutdown; left ZERO core dumps.

## The result (proof from the postmaster log)

```
LOG:  database system is ready to accept connections
LOG:  xtc: carrier scheduler thread up (single-loop app)
LOG:  xtc: spawned backend fiber pid=(loop=0,local=1,gen=1)
LOG:  xtc: B_BACKEND launched as xtc fiber (child_slot=1)
xtc: backend fiber entered; running backend_thread_entry
xtc: fiber wait_fd fd=34 interest=0xd timeout_ms=-1 (via xtc_proc_wait_fd)   <-- repeated
```
```
$ psql -c "select 1 as xtc_carrier"
 xtc_carrier
-------------
           1
(1 row)
psql rc=0
```

- `fd=34` is the backend's client socket (`MyClientSocket->sock`).
- `interest=0xd` = READABLE|HUP|ERR; `timeout_ms=-1` = wait forever.
- Every top-level protocol read parks the fiber on the xtc loop instead of
  blocking the carrier thread in `epoll_wait`.  This is threaded PostgreSQL
  cooperatively scheduled by xtc.

## Multiple backends -- fiber teardown + thread reset (verified 2026-07-04)

Five commits after the single-backend milestone make the carrier reusable
across backends instead of one-shot:

- `373ba1b7` fiber teardown: exit the backend fiber via `xtc_exit_self`
  (`xtc_pg_backend_fiber_exit`) at the point a pthread carrier would call
  `pg_thread_exit`, so the loop reclaims the proc/task slot.
- `79599625` reset the carrier thread's current-work between fibers.
- `7072801b` `PgRuntimeResetThreadForNewBackend()` on each fiber entry --
  flush hot current-cells/mirrors, clear current-work, restore the
  fresh-thread invariant (one carrier OS thread now hosts many fibers in
  sequence, so the previous fiber's thread-locals are still present).
- `b96ef1d8` restore `early_session_fallback` to its pristine initializer
  between fibers (fixes the 2nd-backend timezone/GUC NULL-context crash).
- `4b41779b` save/restore PG current-work across the `xtc_proc_wait_fd` yield
  (`PgRuntimeSaveCurrentWork` / `PgRuntimeRestoreCurrentWork` in
  `xtc_pg_wait_fd`), since the loop may run other fibers on this OS thread
  while one is parked.

### SEQUENTIAL: WORKS (10 backends, spawned=10 exited=10)

A loop of 10 `psql -c "select N"`, each a fresh connection:

```
psql select 1 -> 1        ...        psql select 10 -> 10
```
```
xtc: spawned backend fiber pid=(loop=0,local=1,gen=1)
xtc: backend fiber exiting  pid=(loop=0,local=1,gen=1) code=0
xtc: spawned backend fiber pid=(loop=0,local=1,gen=2)
xtc: backend fiber exiting  pid=(loop=0,local=1,gen=2) code=0
...
xtc: spawned backend fiber pid=(loop=0,local=1,gen=10)
xtc: backend fiber exiting  pid=(loop=0,local=1,gen=10) code=0
seq: spawned=10 exited=10
```

Every backend gets `local=1` reused with a bumped `gen` -- the loop slot is
reclaimed and handed to the next fiber (no slot leak, no hang on the Nth).
This is the fiber-teardown + thread-reset mechanism working exactly as
intended.

### CONCURRENT: PARTIAL -- correct rows, but teardown wedges the loop

4 psql sessions opened at once, each `select N` then holding the socket ~0.4s
so the fibers coexist on the loop:

```
driver wall=1.09s
  psql select 601 -> 601
  psql select 602 -> 602
  psql select 603 -> 603
  psql select 604 -> 604
fibers this run: spawned_delta=4 exited_delta=1
```
```
xtc: spawned backend fiber pid=(loop=0,local=1,gen=11)
xtc: spawned backend fiber pid=(loop=0,local=2,gen=1)
xtc: spawned backend fiber pid=(loop=0,local=3,gen=1)
xtc: spawned backend fiber pid=(loop=0,local=4,gen=1)
xtc: backend fiber exiting  pid=(loop=0,local=1,gen=11) code=0   <-- only ONE exits
```

All four backends run concurrently (distinct `local=1..4` slots spawned in the
SAME millisecond) and every one returns the CORRECT row -- so query execution,
the per-fiber thread-reset, and the socket parking all work under concurrency.
But on disconnect only ONE fiber (`local=1`) resumes and exits; `local=2,3,4`
stay parked forever.  The single loop can then make no progress -- a new
`select` also parks and never returns (loop wedged).  Recovering requires an
immediate stop / SIGKILL.

The boundary is reproducible: **one** parked fiber resumes and exits fine (all
sequential cases, and a lone `pg_sleep(0.3)` backend); **two or more** fibers
parked on the loop at once -> exactly one wakes, the rest are lost.  It is NOT
timer-specific (socket-hold backends wedge identically) and NOT a data problem
(rows are always correct) -- it is a lost-wakeup when multiple fibers are
simultaneously parked via `xtc_proc_wait_fd` on the single carrier loop.

Probable cause (to confirm, NOT yet fixed -- libxtc is read-only here): with N
fibers each parked on their own `set->epoll_fd` + timer + recv-waker, only the
first completion dispatched by the loop wakes its fiber; the others' wakeups
are not delivered (or the resumed fiber does not yield the loop back so the
next ready completion is never reaped).  Candidates: the shared
`__thread xtc_in_backend_fiber` / current-proc restore interacting with more
than one live fiber, or the carrier proc body not re-entering the loop after a
fiber exits while siblings are parked.

### Next steps for concurrent teardown

1. **Instrument the wake path.**  Log, per fiber, entry to `xtc_pg_wait_fd`,
   the `xtc_proc_wait_fd` return code + revents, and the resume -- with the
   fiber pid -- to see whether the lost fibers never get a completion or get
   one and fail to run.  (Repro: 2 concurrent socket-hold backends is the
   minimal wedge.)
2. **Cross-fiber SetLatch wakeups.**  The epoll-fd intercept assumes a
   SetLatch makes the epoll fd readable; verify a second fiber's SetLatch
   actually unparks a first fiber under LISTEN/NOTIFY.  A dedicated latch
   wake into the fiber's mailbox (`XTC_WAIT_MAILBOX`) may be needed instead of
   relying on the epoll-fd edge.
3. **A real carrier POOL.**  The single-loop cooperative model is the
   bringup shape; the Phase-15 target is many loops/threads (`opts.n_loops`
   > 1) so a blocked/parked fiber never starves siblings.  This likely
   sidesteps the single-loop lost-wakeup entirely and is the intended
   architecture.

## Why this worked where the fork spike did not

The fork spike (xtc-m16-1b-spike) died at the read/wait **fd divergence**: a
`dup()` made the backend read fd (12) differ from the fd registered in the
WaitEventSet (3), so the fiber parked forever on the wrong descriptor.

The multithreaded tree already solved that: it keeps the backend **in-thread**.
`postmaster_backend_thread_launch` dup()s the client fd ONCE into
`thread_start->client_sock.sock`; `backend_thread_run_backend` sets
`MyClientSocket = &thread_start->client_sock`; `FeBeWaitSet` is later built from
that same `MyProcPort->sock`.  Read fd == wait fd, always.  The PG-globals wall
that blocked the fork approach is also gone -- the tree runs the full
thread-per-session init.  We reuse ALL of it and change only the carrier.

## The seam (files changed on the branch)

1. `src/backend/postmaster/pg_xtc_carrier.c` / `.h` (new)
   - Single-loop `xtc_app` on a dedicated pthread (`xtc_pg_carrier_start`).
   - `xtc_pg_launch_backend_fiber(entry, arg)` spawns the tree's
     `backend_thread_entry` as an `xtc_proc` fiber on the carrier loop.
   - `xtc_pg_wait_fd(fd, WL_*, timeout_ms)` bridges WL_* <-> XTC_IO_* and calls
     `xtc_proc_wait_fd`.
   - `__thread bool xtc_in_backend_fiber` -- true only on the carrier thread
     while a backend fiber runs.
   - `xtc_set_stack_size(8 MiB)` -- CRITICAL: xtc's default fiber stack is 64
     KiB, which overflows instantly in PG parser/planner recursion.  Matched to
     pg_thread.c's 8 MiB pthread stack.  (Calibration knob -- deepen if a path
     faults.)

2. `src/backend/postmaster/launch_backend.c`
   - For `USE_XTC_CARRIER` + `B_BACKEND`, route to
     `xtc_pg_launch_backend_fiber(backend_thread_entry, thread_start)` instead
     of `pg_thread_create`.  Still publishes the logical backend.

3. `src/backend/storage/ipc/waiteventset.c`
   - `WaitEventSetWaitBlock` (WAIT_USE_EPOLL): when `xtc_in_backend_fiber`, wait
     on `set->epoll_fd` (itself pollable) via `xtc_pg_wait_fd`, then a
     non-blocking `epoll_wait(...,0)` to harvest.  ALL event decoding below is
     reused unchanged -- the smallest possible intercept.

4. `src/Makefile.global.in` (template, so configure regenerates it)
   - `USE_XTC_CARRIER` gate: `-DUSE_XTC_CARRIER -I$(XTC_ROOT)/src/inc` and
     `LIBS += $(XTC_ROOT)/build_unix/libxtc.a -luring -lssl -lcrypto`.

5. `src/backend/postmaster/Makefile`
   - `pg_xtc_carrier.o` when `USE_XTC_CARRIER`.

## Baseline-build fixes (pre-existing multithreaded-tree bugs, gcc 14 + cassert)

Committed separately (commit "fix multithreaded-tree baseline build ...").
These are NOT xtc; they block a clean build of the stock branch under gcc 14.3:

- `nodes/read.c`, `nodes/readfuncs.c`: include `utils/backend_runtime.h`
  (defines `struct PgExecutionNodeIOState`) BEFORE `nodes/readfuncs.h` aliases
  `restore_location_fields` -> accessor macro.  Wrong order lets the object
  macro poison the struct field declaration / recurse through the generated
  `.def` accessor ("declared as a function" / "has no member").
- `nodes/readfuncs.h`, `nodes/outfuncs.c`: pin `restore/write_location_fields`
  to a stable inline over the extern accessor, immune to a later transitive
  `backend_runtime.h` re-expanding the recursive `.def` macro.
- `utils/activity/pgstat_io.c`: include `access/xlog.h` for the
  `track_wal_io_timing` accessor (GUC moved into session runtime state).

The baseline was built **without `--enable-cassert`** for the xtc bringup,
because cassert also trips a pre-existing bootstrap-mode assertion
(`Assert("GUCMemoryContext == NULL")` in guc.c during initdb -- the tree's
GUC-into-session-state refactor is not bootstrap-safe under cassert with gcc
14).  That assertion is orthogonal to xtc; documented as a known gap.

## Exact build recipe (reproducible)

Prereqs: `libxtc.a` at `/home/gburd/ws/xtc/build_unix/libxtc.a`.

```sh
cd /home/gburd/src/multithreaded-postgres     # branch xtc-carrier
export TMPDIR=/scratch/xtc-test; mkdir -p /scratch/xtc-test

nix-shell -p gcc gnumake bison flex perl readline zlib openssl liburing --run '
  ./configure --prefix=/scratch/xtc-test/inst --without-icu --enable-debug \
              CFLAGS="-O1 -g"
  make USE_XTC_CARRIER=1 -j"$(nproc)"
  make USE_XTC_CARRIER=1 install
'
```

Run (fresh cluster, LC_CTYPE=C -- threaded mode requires the db LC_CTYPE to
match the postmaster process LC_CTYPE):

```sh
export PGDATA=/scratch/xtc-test/pgdata LC_ALL=C LC_CTYPE=C LANG=C
export LD_LIBRARY_PATH=<liburing-2.12>/lib   # or run inside the nix-shell
/scratch/xtc-test/inst/bin/initdb -D "$PGDATA" -U postgres --no-locale -E UTF8
cat >> "$PGDATA/postgresql.conf" <<EOF
listen_addresses=''
unix_socket_directories='/scratch/xtc-test'
autovacuum=off
logging_collector=off
EOF
/scratch/xtc-test/inst/bin/pg_ctl -D "$PGDATA" -l /scratch/xtc-test/pm_xtc.log \
    -o "-c multithreaded=on" -w start
LC_ALL=C /scratch/xtc-test/inst/bin/psql -h /scratch/xtc-test -U postgres \
    -d postgres -c "select 1 as xtc_carrier;"
grep -aE "xtc:" /scratch/xtc-test/pm_xtc.log
```

Helper: `/scratch/xtc-test/run_xtc.sh` (NOT in the repo).

Stock baseline confirmed BEFORE xtc: process mode + threaded mode
(`multithreaded=on`) both answer `select 1`; the backend runs as an OS thread
inside the postmaster process (8 threads, zero child processes -- verified via
/proc/PID/task and ps --ppid).

## Known walls beyond this milestone (concrete next steps)

1. **Fiber lifecycle / multiple backends.**  PARTIALLY CLOSED (2026-07-04).
   The fiber-teardown + thread-reset commits (see "Multiple backends" section
   below) make N SEQUENTIAL backends work end to end -- each spawns a fresh
   fiber, returns the correct row, and exits cleanly, freeing the loop slot for
   reuse (verified 10 in a row, spawned=10 exited=10).  What remains OPEN is
   CONCURRENT teardown: two or more backend fibers parked on the single loop at
   once return correct rows but only ONE resumes/exits; the rest stay parked and
   the loop wedges (lost-wakeup).  Next: cross-fiber wakeup / a real carrier
   POOL of many loops -- see "Multiple backends" below for the precise failure
   and next steps.

2. **Startup-process teardown crash (pre-existing, flaky, NOT xtc).**  After a
   non-clean shutdown, recovery runs and the StartupProcess thread SIGSEGVs in
   `proc_exit -> PgBackendResetXLogClosedState -> MemoryContextDelete`
   (backend_runtime_teardown.c:569).  This is in a normal pthread carrier, not
   the xtc fiber.  It corrupts the cluster and forces a re-initdb.  Reproduce on
   the pristine `multithreaded` branch and report upstream; use clean
   `pg_ctl -m fast stop` to avoid triggering recovery meanwhile.

2b. **Late io-worker autoscale does not launch (pre-existing, NOT xtc, NOT
   libxtc).**  Under `multithreaded=on`, raising `io_min_workers` at runtime
   (`ALTER SYSTEM SET io_min_workers = 3; SELECT pg_reload_conf();`) does not
   start a new io worker: the count stays at the startup value and no
   additional "starting io worker thread carrier" appears, even with active
   AIO-generating load over 40s.  Verified DETERMINISTIC against BOTH libxtc
   v1.0.0 and v1.1.0 (same fresh trees on meh), so it is not a libxtc version
   effect.  io workers run as base pthread carriers here (B_IO_WORKER is not
   yet fiber-eligible), so this is the multithreaded tree's late-io-worker
   autoscale/launch-request path.  Report upstream.  The
   `001_threaded_runtime.pl` "late IO worker" check is a TODO until it is
   fixed; the earlier one-off 129/129 pass was a lucky timing window.

2c. **RESOLVED / MISDIAGNOSED (libxtc v1.2.1).**  We reported an "intermittent
   SIGSEGV inside xtc_exit_self during teardown" because monitored backend
   fibers delivered a DOWN with reason -11 after a clean exit.  The libxtc team
   showed this was NOT a fault: -11 was XTC_E_NOTFOUND (the monitor raced a
   short-lived backend's clean xtc_exit_self(0) and landed just after it
   exited), and XTC_E_NOTFOUND == -11 collides with -SIGSEGV, which is what
   made us read it as a crash.  xtc_exit_self teardown never faulted; R1 was
   catching nothing.  v1.2.1 delivers the monitor-of-already-dead DOWN with a
   DISTINCT reason XTC_DOWN_NOPROC (-100000, outside the signal range), so
   "already gone" is unambiguously not a crash.  Our supervisor now classifies
   via xtc_down_is_noproc() and treats NOPROC as benign.  The suspected
   shutdown slowdown / worker wedge were side effects of escalating on the
   benign NOPROC; expected to disappear now (to confirm on meh).

2d. **RESOLVED (libxtc v1.2.1).**  We reported that a backend fiber which
   SIGSEGVs very early in its body (before its first yield / before arming a
   recovery frame) delivered no monitor DOWN.  libxtc confirmed the gap: R1
   containment only fired once the proc had called xtc_proc_recovery_arm(); an
   early fault fell through to escalation and delivered neither a DOWN nor a
   crash.  v1.2.1 AUTO-ARMS a default recovery frame at proc entry, so ANY
   contained fault -- including in the first statement -- unwinds that one proc
   and delivers a DOWN with the POSITIVE signal number (11 for SIGSEGV).  Two
   invariants preserved: a stack-overflow fault still kills the process (guard
   page), and a fault inside a critical section still aborts (PANIC semantics).
   So PG_XTC_INJECT_CRASH now yields a DOWN reason=11, which the supervisor
   classifies as a genuine crash and escalates (to confirm end-to-end on meh).

2e. **RESOLVED (our side): end-to-end genuine-crash escalation validated on
   meh (v1.2.1, 24-loop pool).**  The injection escalation did not fire because
   the carrier never called xtc_fault_guard_install() -- so no
   SIGSEGV/SIGBUS/SIGFPE/SIGILL handler + alt-stack was registered on the loop
   threads, and libxtc's auto-armed recovery frame had no handler to unwind the
   faulted fiber and deliver its DOWN.  (This was NOT a spawn/monitor race: the
   supervisor's xtc_monitor returned rc=0 before the child ran; and NOT the
   auto-arm timing: the frame is armed at proc entry before the body.)  libxtc's
   own tests install the guard per loop thread; we now do too, at supervisor
   entry (one supervisor fiber per loop thread; idempotent).  This is required
   for containment of REAL backend-fiber crashes, not just the test.  Result:
     - Injected fault -> DOWN reason=11 (positive SIGSEGV) -> GENUINE-CRASH ->
       "terminating threaded server runtime after backend fiber crash" ->
       postmaster EXITED (escalated).  INJECT=1, GENUINE=1, escalation=1.
     - Normal 24-loop ops: 11 clean DOWNs (reason 0), GENUINE=0, escalation=0,
       NOPROC=0, smoke 11/11.  No false escalation from the guard.
   #7 Stage 1b (crash detection + escalation) is fully validated end-to-end.

3. **Latch/SetLatch cross-fiber wakeups.**  CLOSED (2026-07-06, 8-loop pool on
   floki).  The wake path is: `NOTIFY` on fiber A -> `SetLatch(B)` sees a
   sibling-thread owner (`owner_pid == MyProcPid`, different carrier thread) and
   calls `WakeupOtherProcFd(latch->owner_wakeup_fd)`.  On this tree WL_LATCH_SET
   with `WAIT_USE_EPOLL` defaults to `WAIT_USE_SELF_PIPE`, so `owner_wakeup_fd`
   is B's per-fiber self-pipe write fd (`GetWaitEventSetLatchWakeupFd()` ->
   `selfpipe_writefd`, a per-current-work cell).  The write makes B's self-pipe
   read fd -- a registered epoll event in B's set -- readable, which makes B's
   `set->epoll_fd` readable, which returns B's `xtc_pg_wait_fd` and unparks the
   fiber.  Verified: a parked `LISTEN xtc_chan; pg_sleep(2)` backend on one loop
   woke and reported the async notification when a *different* backend (a
   different fiber/loop) issued `NOTIFY xtc_chan`.  Smoke step 3 = PASS.

4. **sigsetjmp / ereport(ERROR) inside the fiber.**  CLOSED (2026-07-06,
   8-loop pool on floki).  `SELECT 1/0` inside a fiber raised "division by
   zero", the `sigsetjmp`/`PG_TRY` unwind ran on the fiber stack
   (`PG_exception_stack` is per-thread and holds for the fiber's lifetime), and
   the SAME session then ran `SELECT 'recovered'` successfully -- the fiber
   survived the ERROR unwind and stayed usable.  Smoke step 4 = PASS.

5. **cassert build.**  Fix the bootstrap `GUCMemoryContext == NULL` assertion so
   the xtc carrier can run under `--enable-cassert` (better crash diagnostics).

6. **Remove diagnostic writes.**  The rate-limited raw `write()` proofs in
   `pg_xtc_carrier.c` (fiber-entered, wait_fd) should be dropped once the
   lifecycle work lands.

## Commits on branch xtc-carrier

- `xtc-carrier: fix multithreaded-tree baseline build under gcc 14 + cassert`
- `xtc-carrier: wire xtc fiber carrier for threaded B_BACKEND (USE_XTC_CARRIER)`
- `xtc-carrier: SUCCESS - select 1 round-trips through xtc scheduler`
- `xtc-carrier: fiber teardown - exit backend fiber via xtc_exit_self, not pg_thread_exit`
- `xtc-carrier: reset carrier thread current-work between fibers (fixes 2nd backend GUCMemoryContext NULL crash)`
- `xtc-carrier: reset carrier thread to fresh-thread state on each fiber entry (PgRuntimeResetThreadForNewBackend)`
- `xtc-carrier: restore early_session_fallback between fibers (fixes 2nd backend timezone/GUC NULL-context crash)`
- `xtc-carrier: make PG current-work fiber-local across the wait yield (concurrent backends)`
- `xtc-carrier: verify multi-backend outcome (B PARTIAL) - N sequential works, concurrent teardown wedges`
