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

## Session 1 -- Phase 15 Gate F suite run on real EC2 hardware (2026-07-10)

Built + ran the protocol-scheduler TAP on c7i.metal-24xl (96 vCPU, us-east-2,
libxtc v1.10.0) via a new reusable harness (scripts/run-suite-ec2.sh) -- this
removes the meson-on-btrfs dev-env TAP hang that blocked every prior gate.

Results (test_backend_runtime TAP):
- 005 Phase14 protocol scheduler: OK (35); 006 Phase14 pm-death: OK (3);
  007 Phase15 pooled mode: OK (46); 008 Phase15 pooled pm-death: OK (11).
- 004 Phase13 wait-completion + 009 deep-waits-pinned SKIP in the default build
  (PG_RUNTIME_ENABLE_WAIT_COMPLETION_PUBLICATION is diagnostic-only, compiled
  out in production).  A second build with -DPG_RUNTIME_ENABLE_WAIT_COMPLETION_
  PUBLICATION: 004 OK (46); 009 FAIL (5 of 33).

So Phase 14 + the pooled happy path pass on real hardware; Phase 13 passes with
its diagnostic publication compiled in; 009 (deep-waits-pinned) surfaced a REAL
Phase 15 bug -- the Session-2 (hardening) target, found early per directive-4.

009 failure = a pooled backend in a deep event-set wait while holding an LWLock
(test_backend_runtime_hold_lwlock(60000)) does NOT publish its wait-completion
(snapshot inactive|none) and its protocol-park snapshot shows committed|polling
-- i.e. it looks PROTOCOL-PARKED instead of carrier-pinned, violating the Phase
15 "deep waits remain carrier-pinned" invariant; and pg_cancel of it times out
(48s).  The advisory-lock deep-wait cases just above (ProcSleep->WaitLatch) PASS
and stay pinned, so the gap is specifically the C-level event-set wait path
(WaitEventSetWait->PgSuspend) under pooled mode.  Full detail:
/tmp/session1-gatef-results.md.  EC2 instance i-0d4b5c5d575ca4c6a kept for the
Session-2 investigation; MUST terminate + delete key/SG when done.

### Session 2 progress: 009 diagnosis narrowed (2026-07-10)

Instrumented the box's build-wc (waiteventset.c XTCDBG_DW at the publish gate).
Re-ran 009.  Findings:
- The LWLock holder's WaitLatch does NOT hit the no-publish diagnostic -- so
  PgBackendShouldPublishWaitCompletion returned TRUE for it and it entered the
  PgSuspend publish path.  The only no-publish logs are checkpointer/bgwriter
  (thread_backed=0, correct).  So the holder DID attempt to publish, yet the
  test's snapshot read "inactive|none".
- The protocol-park snapshot for the holder = "committed|polling": PARK_STATE=
  COMMITTED, QUEUE_STATE=POLLING.  But test_backend_runtime_hold_lwlock runs
  during ExecutorRun where doing_command_read=false, and protocol-parking only
  commits when doing_command_read=true (postgres.c ~5089/6903).  A running
  command must show park_state=NONE.  And QUEUE_STATE=POLLING (not LEASED) is a
  backend the scheduler is still polling -- PgRuntimeProtocolSchedulerLeaseBackend
  explicitly refuses to lease a POLLING backend.
- Registry lookup is by backend->id (a monotonic counter, PgBackendAssignId),
  NOT MyProcPid; the test passes pg_backend_pid().  The advisory-lock deep-wait
  case (tests 16-21) uses the SAME pg_backend_pid() path and PASSES, so a plain
  id-mismatch is not the whole story -- the differentiator is the wait TYPE
  (advisory = ProcSleep->WaitLatch publishes+pins fine; direct WaitLatch while
  holding an LWLock does not).

SHARPENED HYPOTHESIS: the snapshot is reading a backend that is still in the
scheduler's POLLING queue with stale COMMITTED park state -- either (a) the
holder session started running its command WITHOUT going through
PgBackendResumeProtocolReadPark (which clears park_state to NONE), leaving stale
committed|polling, and its wait-completion is published on the RUNNING backend
object while the snapshot-by-id resolves a stale/other entry; or (b) pg_backend_pid
in pooled mode does not map to the running holder's backend->id the way it does
for the advisory case.  NEXT decisive probe (not yet run): log at
PgWaitCompletionPublish (backend->id + we) and at the snapshot lookup (requested
id -> resolved backend->id), run 009, and confirm whether publish and snapshot
agree on the id, and whether a resume path ran without clearing park_state.
Instrumentation for that is staged on the EC2 box (~/xtcsrc), not in the tree.
This must be root-caused before pooled becomes the default (Session 3).

### Session 2 -- decisive publish/snapshot id probe run (2026-07-10)

Ran always-on publish+snapshot id probes on build-wc, re-ran 009 (657 DW lines).
Ruled OUT the id-mapping hypothesis: pg_backend_pid() returns
PgBackendGetSignalPid() which in threaded mode returns (int) backend->id, and
ThreadedBackendRegistry is keyed by backend->id -- so the snapshot-by-id
resolves the CORRECT backend (observed: req_id=11 resolved_id=11 we=100663297
kind=1 state=1 for the HoldLWLock event -- a correct publish+resolve).

So the completion IS published and resolvable, but transitions WAITING->cleared
while the single WaitLatch(60000) is still pending (the test's polling snapshot
usually catches it already inactive).  Since WaitLatch calls WaitEventSetWait
exactly once and the SQL function calls WaitLatch once, the callback
(WaitEventSetWaitInternal) must be RETURNING early and PgSuspend clearing the
shared backend->wait_state completion -- consistent with the protocol-park
snapshot committed|polling: in pooled mode the deep mid-command WaitLatch is
being treated as a park/yield point (fiber yields to the scheduler), the
PgSuspend PG_TRY completes and clears the completion, and the backend sits in
the scheduler POLLING queue with stale COMMITTED park state.

LOCALIZATION (strong): the bug is the pooled-mode wait-completion publish/clear
lifecycle across a deep event-set wait -- a single backend->wait_state slot whose
completion is cleared when the deep WaitLatch's fiber parks, so a snapshot during
the park sees inactive, and the backend is left committed|polling instead of
carrier-pinned.  The advisory-lock deep wait (ProcSleep->WaitLatch) passes
because it does not hit this park/clear cycle the same way.

NEXT (fresh focused pass, NOT the 33-subtest TAP): a MINIMAL standalone repro --
one pooled session running test_backend_runtime_hold_lwlock, one observer doing
repeated snapshots -- with timestamped publish / clear / park-commit / resume
logs correlated, to confirm whether (a) the WaitLatch callback returns+clears
mid-wait (park treating a deep wait as a yield -- the invariant bug), or (b) a
nested PgSuspend on the same wait_state slot clobbers it.  Fix accordingly
(likely: a deep mid-command wait must NOT park/clear in pooled mode -- keep it
carrier-pinned per the Phase 14/15 "deep waits remain carrier-pinned" rule).
Instrumentation staged on the EC2 box; local tree clean.  Instance
i-0d4b5c5d575ca4c6a kept running for continuation.

### Session 2 -- CONFIRMED real bug (clean build), not a staleness artifact (2026-07-10)

The deep-dive traces were partly confounded by tmp_install .so staleness (the
diagnostic build mixes an incrementally-rebuilt postgres with a possibly-stale
test-extension .so), which made the publish/clear/park-commit timeline
unreliable.  To remove all doubt, built a FRESH clean build-wc2 (meson setup +
full ninja, -DPG_RUNTIME_ENABLE_WAIT_COMPLETION_PUBLICATION, pristine HEAD
sources) and ran 009: it STILL FAILS the same 5 subtests --
  - pooled LWLock holder publishes event-set wait completion
  - pooled LWLock holder remains carrier-pinned and non-protocol-parked
  - pooled LWLock semaphore wait publishes wait completion
  - pooled LWLock semaphore wait remains carrier-pinned and non-protocol-parked
  - canceled pooled LWLock holder releases test LWLock
So it is a GENUINE Phase 15 bug in our code, reproducible on a clean build; not
a test/build artifact.

CONFIRMED FACTS:
- A pooled backend in a deep LWLock-associated wait (test_backend_runtime_hold_
  lwlock's WaitLatch, and the LWLock semaphore wait) does NOT expose an
  observable event-set/semaphore wait-completion (snapshot reads inactive|none),
  and its protocol-park snapshot reads committed|polling instead of the required
  carrier-pinned none|none.
- The advisory-lock deep wait (ProcSleep->WaitLatch) in the SAME test PASSES and
  stays pinned -- so the defect is specific to the WaitEventSetWait/latch +
  LWLock-context deep wait under pooled mode, not all deep waits.
- id-mapping is NOT the cause (pg_backend_pid == backend->id == registry key,
  verified; a correct publish+resolve for the HoldLWLock event was observed).
- It is OUR code (pooled protocol scheduler + wait-completion/park lifecycle),
  NOT libxtc: libxtc only provides the fiber park; the park/pin decision and the
  wait-completion publish/clear are PostgreSQL-side.

RELIABLE REPRODUCER: build-wc2 on the EC2 box (clean, flagged); 
`meson test -C build-wc2 test_backend_runtime/009_phase15_pooled_deep_waits_pinned`.

NOT YET FIXED: the exact defect (why a mid-command LWLock-context deep wait
reaches committed|polling / loses its published completion, while advisory does
not) needs clean isolation on build-wc2 WITHOUT the stale-.so confounder --
trace the session-loop park decision (PgSessionRunProtocolSchedulerUntilBoundary
/ PgBackendPrepareProtocolReadPark) and doing_command_read at the moment the
holder is snapshotted committed|polling.  MUST be fixed before pooled becomes
the default (Session 3).  Deliberately NOT guessing a scheduler-pin fix without
clean isolation -- a wrong fix to the pin invariant is worse than a documented,
reproducible open bug.

### Session 3 -- pooled protocol mode is now the DEFAULT (2026-07-10)

Commits: a8a55f36910 (default flip) + ec2ba838555 (bundled-module affine batch).

The flip: pooled_protocol_carriers boot value is now -1 ("auto").  In the
postmaster, after SelectConfigFiles() (mirroring the io_method=worker->xtc
remap), -1 resolves under multithreaded=on to a bounded pool (min(online CPUs,
8), <=MaxConnections, >=1) and logs "pooled protocol scheduler enabled with N
carriers (default)"; under multithreaded=off it resolves to 0 so no consumer or
SHOW sees the sentinel in process mode.  Explicit 0 = thread-per-session (still
supported); explicit N respected.  Guarded by USE_XTC_CARRIER; process mode
untouched.  Local smoke confirmed all three: default->8 pooled, 0->tps, 3->3.

Test intent preserved: TAP 001-006 and the check-threaded / check-threaded-
workers regress configs were written for thread-per-session and are now pinned
pooled_protocol_carriers=0.  Added threaded_pooled.conf + a check-threaded-
pooled target (pooled_protocol_carriers=4) so the new default gets full core
regression coverage.

The backend-model gate surfaced by the flip: modules marked THREAD_PER_SESSION
are refused under pooled-affine (ordinal module_model >= required_model, AFFINE=3
> TPS=1).  Full core regression under pooled: 114/245 failing initially,
dominated by "backend model mismatch".  Audited + upgraded the affine-safe
bundled modules to POOLED_PROTOCOL_AFFINE (plpgsql -- already fully session-
scoped via plpgsql_current_session_state(); 21 encoding conversion procs;
dict_snowball; libpqwalreceiver; regress.c).  114 -> 38 (after plpgsql) -> 8
failures, 0 model-mismatch, 0 crashes.  The residual 8 (join_hash, tidrangescan,
incremental_sort, select_parallel/write_parallel/vacuum_parallel, bitmapops,
tsearch) are pre-existing threaded-mode parallel-query / plan-shape diffs, a
strict subset of the thread-per-session baseline (which fails 120/245 on the
SAME schedule) -- so pooled introduces zero new regressions and is markedly
cleaner than TPS.

Deliberately left on THREAD_PER_SESSION (Session 4 contrib+PL batch, or
intentionally testing the gate): pgrepack, plsample, worker_spi, test_shm_mq,
test_ext_threaded.

Open for Session 4/5: triage the residual 8 parallel/plan diffs (likely parallel
worker behaviour + plan costing under threaded, common to TPS); the two pooled
poll() sites were reviewed and are NOT blockers -- the sticky-idle poll is
already fiber-guarded (!xtc_in_backend_fiber), and the scheduler reactor poll at
postgres.c ~6143 is the carrier's own between-sessions reactor (10ms bound, not
inside a fiber), inherent to the affine model where a mid-command deep wait
pins its carrier and multiplexing happens at protocol-read boundaries.

### Session 2 -- 009 FIXED (2026-07-10, commit 223663b9d93)

Root cause (traced instrumentation-free via a SetLatch() backtrace probe on the
clean build-wc2): the LWLock holder's WaitLatch(MyLatch, WL_LATCH_SET|WL_TIMEOUT,
60000) returned WL_LATCH_SET in *0-1ms* instead of blocking -- MyLatch was
ALREADY set at wait entry (XTCDBG_ENTRY ... MyLatch.is_set=1).  The backtrace
pinned the set to InitProcess()+0xb4c, called from
BackendStartSessionWithStartupData().  InitProcess() sets MyLatch during proc
setup so a mid-init signal is not lost.  An affine pooled session then runs its
first command directly through backend_pooled_protocol_run_attached_logical() ->
PgSessionRunProtocolSchedulerUntilBoundary(), with no client startup/auth
round-trips to cycle the process latch in between, so the stale set survives
into the command's first WaitLatch.  hold_lwlock issues a SINGLE un-looped
WaitLatch and thus falls straight through; pg_sleep/ClientWrite/advisory all
loop internally until their real deadline/condition and so tolerate a spurious
wake -- which is why only the LWLock cases failed.  The collapsed holder then
released its LWLock (num_held_lwlocks=0 at park-commit) and idle-parked
(committed|polling), so the observer saw inactive|none and a non-pinned park.

Fix: ResetLatch(MyLatch) once in the affine entry
(backend_pooled_protocol_run_attached_logical caller in launch_backend.c) right
after BackendStartSessionWithStartupData() returns and before the command loop.
Guarded by #ifdef USE_XTC_CARRIER + the affine-pooled path, so process mode and
thread-per-session are untouched by construction.  The resume paths already
Reset on WL_LATCH_SET wakes; this closes the first-command gap they missed.

Evidence on build-wc2 (clean, wait-completion flag): 009 now OK, 33/33 subtests.
Full test_backend_runtime suite (flagged): 002/004/005/006/007/008/009 all OK
(no regression).  001_threaded_runtime, 003_milestone_w_core_smoke, and regress
fail with `timed out waiting for startup IO workers` / exit 29 -- a PRE-EXISTING
test-vs-design conflict (001 sets io_method=worker and asserts 2 forked `io
worker` backends, but multithreaded=on remaps io_method=worker->xtc so no io
workers are forked), NOT a regression from this fix (which touches only
launch_backend.c, unrelated to io workers; prior commits already flagged 001's
io-worker autoscale as a base-tree TODO).  No cores/PANIC/asserts.

## libxtc v1.7.0 adopted; cross-thread fd-wake-miss is the Tier B blocker (2026-07-08)

Bumped to libxtc **v1.7.0** (rev 17ee625; commit 1e31ff60bb1), via v1.6.0 (rev
618adcf; commit 927e27c4cf4).  Both flake-only (headers API-unchanged).  v1.6.0
added a fiber-aware left-right lock (xtc_alrlock, additive/unused here) + docs;
v1.7.0 fixes the xtc_exec_fini cross-thread-spawn teardown leak (primary path;
small timing-dependent residue remains) and moves the suite to official
hegel-c.  Smoke 20/20 on both.

**Tier B ROOT CAUSE FOUND: cross-thread fd-wake-miss on a parked fiber.**
Deep investigation of the standby fiber-worker shutdown wedge traced the real
fault below shutdown: a **hot-standby client backend fiber hangs forever during
InitPostgres** on a fresh connection (even `SELECT 1`).  That is why the standby
never shuts down cleanly -- a hung backend never exits, so PM_WAIT_BACKENDS
never converges.  The earlier "standby shutdown ordering" framing was a symptom;
the cause is a lost wakeup.

Chain (all verified, io_method=worker default):
  - The backend fiber reaches InitPostgres, must read an uncached catalog page,
    submits it as async I/O to a (thread-carrier) I/O worker, and parks on the
    AIO-completion ConditionVariable -> WaitLatch(MyLatch, timeout=-1).
  - The I/O worker thread DOES perform the read, transition the IO to
    COMPLETED_SHARED, and SetLatch the backend (WakeupOtherProcFd writes the
    backend loop's self-pipe write fd).
  - /proc on the hung standby confirmed the plumbing is correct: the epoll fd
    watches the self-pipe READ fd for EPOLLIN; read fd and write fd are the same
    pipe inode; the latch owner_wakeup_fd is that write fd.
  - Yet the fiber (parked in xtc_proc_wait_fd on the epoll fd; carrier loops in
    io_uring_get_cqe) never resumes.  The cross-thread readiness / wake is LOST.
  - DECISIVE isolation: changing ONLY the fiber's infinite AIO-completion wait
    to a 100ms bounded wait makes the standby connect succeed every time.  So
    completion + state transition happen; only the wake delivery is lost.

This is a **libxtc bug** (same class as the v1.4.2 idle-loop wake-miss, but for
a loop parked in xtc_proc_wait_fd on an fd with cross-thread readiness).  It is
NOT in v1.7.0 KNOWN_ISSUES.  Reported in full with runtime evidence, the
investigation trail, and a standalone (non-PG) reproducer shape in
**/tmp/libxtc-crossthread-fd-wake-miss.md**.

Why the PRIMARY works: at connect its catalog pages are already cached, so no
async read is needed; io_method=sync also works (no I/O worker, no cross-thread
completion).  So this bites any fiber backend that blocks on an async I/O
completion driven by another thread -- a general fiber+AIO gap, not a
standby-only or Tier-B-only issue.  It is the gating blocker for Tier B
(WAL_RECEIVER/SLOTSYNC_WORKER): a standby cannot serve fiber backends until the
wake is delivered.

Options once libxtc fixes the wake: re-admit Tier B and validate.  We are NOT
shipping any interim -- no bounded-timeout on the fiber AIO wait (it masks the
libxtc defect and pays latency/wakeups forever), no io_method=sync dodge, and
NOT using io_method=xtc (item #6) to paper over the wake bug (it is separate,
independently motivated work; using it to hide this defect would bury it).
Wait for the libxtc fix.  The gap + the exact contract libxtc must satisfy are
written up in /tmp/tier-b-fiber-aio-gap.md.

### libxtc v1.8.0: xtc_proc_wake wired; Tier B root cause = fork/fiber model (2026-07-09, commits 0da989cea44 + dbaf5c9580b)

libxtc v1.8.0 (rev 0d625f8) shipped the fix for our reported cross-thread
fd/latch wake miss: a new public primitive **xtc_proc_wake(pid)** the embedder
calls from any OS thread after making a watched condition true, to poke the
target loop so a fiber parked in xtc_proc_wait_fd re-checks (exactly the
"poke the target loop" call our report asked for; see libxtc
test/concurrency/test_proc_wake_crossthread.c for the intended pattern, which is
modeled on our PG case).  Bumped the flake (commit 0da989cea44); core path
clean (threaded startup, SELECT 1, fast stop, 0 cores); the primitive is
additive/unused until wired, so process/non-fiber modes are unchanged.

WIRING DONE + ROOT CAUSE RESOLVED (2026-07-09, commit dbaf5c9580b).  Wired
xtc_proc_wake into SetLatch's same-process branch (owner_fiber_* on struct Latch,
captured at the WaitEventSetWaitBlock park point) plus a durable fiber gate
(xtc_self() instead of the racy __thread flag).  Validated: threaded SELECT 1,
LISTEN/NOTIFY cross-fiber wake, loop-not-wedged, error-unwind, fast stop all
clean; process mode unaffected.

BUT the standby hot-backend AIO hang PERSISTS, and proper GDB + instrumented
tracing found the REAL root cause -- it is NOT a libxtc gap and NOT a wiring
detail; it is our FORK/FIBER MIXED MODEL:
  - /proc on a hung standby: the postmaster process (Tgid 134042) runs client
    backends as FIBERS on its carrier-loop THREADS, but io worker 0/1,
    checkpointer, and background writer are FORKED PROCESSES (distinct pids
    134043-46).  They start at maybe_start_io_workers() during
    UpdatePMState(PM_STARTUP) -- BEFORE carriers exist -- so they take the fork
    path and are never relaunched as thread carriers.
  - Trace: backend fiber (MyProcPid=134042) parks on its AIO-completion CV;
    io worker (MyProcPid=134044, a fork) completes the IO and
    ConditionVariableBroadcast -> SetLatch(&backend->procLatch).  In SetLatch,
    owner_pid(134042) != MyProcPid(134044) -> the CROSS-PROCESS branch ->
    WakeupOtherProc(134042) = kill(134042, SIGURG) to the postmaster process,
    NOT the carrier-loop thread hosting the fiber.  Lost wakeup.
  - xtc_proc_wake CANNOT fix this: it only works from WITHIN the process that
    hosts the libxtc runtime.  A forked io worker is a separate address space
    with no handle to the postmaster's loops.  You cannot wake an in-process
    fiber from a foreign process without explicit IPC.
  - Primary works only because its connect-time catalog pages are cached (no
    async read); io_method=sync works because there is no cross-process
    completion.

CONCLUSION (answers "should we transition threads to fibers/procs? should
postmaster_backend_thread_launch use the libxtc model?"): YES.  The completers
must run IN-PROCESS as thread carriers, not forks.  launch_backend.c already
routes B_IO_WORKER through postmaster_backend_thread_launch() when
postmaster_thread_carriers_started, but io workers launch before carriers exist,
so the missing piece is the early-start process->thread HAND-OFF (retire the
forked io workers once carriers are up and relaunch them as thread carriers) --
the same mechanism the launch_backend.c:430 comment promises for
checkpointer/bgwriter (Tier D).  This is blocked by the bringup invariant that
the startup process is forked at PM_STARTUP and starting carriers earlier makes
fork-without-exec unsafe, so it is genuine Tier D/F work, not a quick change.
Full analysis: /tmp/tier-b-fork-vs-fiber-rootcause.md.

INTERIM (correct, not a hack, if the hand-off is deferred): io_method=xtc does
issuer-synchronous AIO on the backend's OWN fiber (no foreign completer at all;
method_xtc.c, already implemented for reads/writes), so a fiber backend never
waits on a cross-process completion.  Routing fiber backends to io_method=xtc
(or sync) under multithreaded=on sidesteps the cross-process wake entirely.  Not
yet applied -- a deliberate decision pending.  NOTHING further is needed from
libxtc: v1.8.0's xtc_proc_wake is the right primitive and is already wired for
the in-process case.

RESOLUTION (2026-07-09, commits 2c9749b2da2 + 42fe3a99469 + 5df28304993, then
switched to sync in 29b8ea4d365): applied the interim (io_method=worker -> SYNC
under multithreaded=on, by rewriting the GUC value after SelectConfigFiles) and
RE-ADMITTED all of Tier B.
Validated: standby fiber backends serve queries (replicated_rows=30000);
WAL_RECEIVER launches as a fiber + streams; SLOTSYNC_WORKER launches once (no
relaunch churn) + syncs; both fast AND immediate stop of standby+primary clean
(0 cores, 0 SIGKILL escalations).  Process mode byte-for-byte untouched.

On the "real fix" (io workers as in-process thread carriers): the hand-off
machinery ALREADY EXISTS and WORKS on a PRIMARY.  Verified with an
XTC_ALLOW_WORKER_IO escape hatch (since removed) + explicit io_method=worker:
maybe_handoff_io_workers() SIGUSR2's each forked io worker at PM_RUN, they exit,
and the relaunch (carriers now up) takes the thread-carrier path -> io worker
FORKS=0, they run as THREADS inside the postmaster, and SELECT 1 works (the
in-process completion SetLatch is same-process cross-thread, woken by the wired
xtc_proc_wake).  BUT it does NOT cover a STANDBY: maybe_handoff_io_workers()
is gated on pmState == PM_RUN, and a hot standby stays in PM_HOT_STANDBY (only
reaches PM_RUN after promotion), so its io workers are never handed off; and io
workers start at PM_STARTUP before carriers exist regardless.  A standby with
io_method=worker therefore still hangs (backend waits on an io worker that is
either a fork that cannot wake it, or absent).  So the interim (now io_method=
SYNC) is the MORE COMPLETE correctness solution today; making io_method=worker
work on standbys would require handing off at PM_HOT_STANDBY too (future Tier D/F
completion), after which the interim remap could be dropped.

XTC INTERIM: SYNC (briefly), THEN XTC AGAIN AFTER THE CONCURRENCY FIX.  The
first interim used io_method=xtc (issuer-async, parks the fiber, no foreign
completer -- the ideal path).  It hit an INTERMITTENT SIGSEGV under CONCURRENT
reads: a backend fiber crashed in BufferLockAcquire -> GetPrivateRefCountEntry
(bufmgr.c:6203, 'entry->data.lockmode = mode', entry bogus), sometimes as
'could not read blocks ... read only 0 of 16384 bytes'.  So the interim was
temporarily switched to sync (commit 29b8ea4d365; deterministically correct but
blocks the carrier loop during a read).

ROOT CAUSE + FIX (commit c8077b2c62b): it was NOT libxtc, NOT the AIO
transport, NOT parallel query -- it was a MISSING current-work save/restore in
our method_xtc.c integration.  xtc_aio_preadv/pwritev PARK the issuing fiber for
the IO (yield to the loop), so the loop runs OTHER backend fibers meanwhile,
which clobber PG's per-backend current-work thread-locals (the hot-field refs
behind CurrentPgBackend/Session/Execution -- including PrivateRefCountArray).
pgaio_xtc_submit did not save/restore that state across the park, so a resumed
fiber used a sibling's PrivateRefCountArray and dereferenced a bogus
PrivateRefCountEntry.  (io_method=sync never parks -> no sibling interleaves ->
why sync was clean.)  Fix: wrap the xtc_aio_preadv/pwritev calls in
PgRuntimeSaveCurrentWork()/PgRuntimeRestoreCurrentWork(), exactly as
xtc_pg_wait_fd() already does for the waiteventset park.

Captured the real backtrace by temporarily skipping xtc_fault_guard_install()
(so the SIGSEGV dumped a core with libxtc/PG frames instead of being
R1-contained) -- the guard install is restored.  Verified after the fix: 20
rounds x 8 concurrent uncached seq scans -> 0 cores (was crashing ~round 4);
autovac churn + parallel seq scans -> 0 cores; full xtc smoke 24/24.  So the
interim is BACK TO io_method=xtc (commit 1ab8c518939), which preserves
carrier-loop concurrency.  NOTHING here was a libxtc defect -- this closes the
prior TRACKED xtc-concurrency item AND unblocks Tier F (io workers as fibers can
now park on xtc IO without corrupting sibling fibers).

XTC IS THE THREADED-MODE IO DESIGN, NOT A STOPGAP (2026-07-09).  On reflection
the io_method=worker->xtc remap under multithreaded=on is not really an
"interim": routing every buffered read/write through the issuing fiber's own
cooperative issuer-async AIO (xtc_aio_preadv/pwritev, parks the fiber, no forked
worker pool, no cross-process wake) IS the natural threaded-runtime IO model.
The remap covers ANY io_method=worker (explicit or default), so there is no
footgun where a user gets the hanging forked-worker path.  Completeness:
  - PGAIO has only READV/WRITEV ops; the xtc method handles both, so it covers
    the entire AIO surface (no FSYNC op exists in this subsystem).
  - WRITE path validated under concurrency: 12 rounds x 6 concurrent write
    churners + CHECKPOINT + parallel workers on io_method=xtc -> alive, crash=0,
    fast stop clean, 0 cores (matches the read-path validation).
  - WAL fsync + WAL writes still use direct pg_fsync/pg_pwrite (blocking
    syscalls on the fiber's carrier loop), NOT the AIO method -- correct but a
    perf note: a fiber briefly blocks its loop during fsync.  Routing fsync
    through xtc is a future perf optimization (item #6 step 4), not a
    correctness gap.
  - io_method=worker's separate worker-pool parallelism is largely redundant in
    threaded mode (each fiber already does async IO); io-workers-as-fibers
    (Tier F) + the standby PM_HOT_STANDBY hand-off are only needed if that
    specific model is ever wanted.  Not building it speculatively.

### Cassert threaded smoke found a worker-fiber entry race (2026-07-08, commit 1168e5c6dc2)

Unblocked work while Tier B waits on libxtc: rebuilt build-xtc-cassert (cassert +
xtc, v1.7.0) and ran the smoke under assertions.  It surfaced a real race the
non-cassert smoke passes clean:
Assert("PMChildFlags[slot] == PM_CHILD_ASSIGNED") in RegisterPostmasterChildActive
(pmsignal.c:306) via InitProcess() in an autovac WORKER FIBER, plus the matching
non-cassert symptoms ("autovac churn fast stop HUNG", spawn/exit mismatch).

Root cause: ReapOrphanedThreadedWorker (postmaster) reaps a worker whose
fiber_entered==0 by publishing a synthetic exit that RELEASES the PMChild slot,
but the reaper's fiber_entered read and the fiber's fiber_entered write were not
synchronized -- the reaper could read 0 while the just-scheduled fiber set
fiber_entered=1 and ran InitProcess->RegisterPostmasterChildActive on the
already-released slot.  Fixed with a dedicated single-winner start_claimed
exchange (distinct from exit_claimed so the exit path is untouched): fiber writes
fiber_entered, barrier, exchanges start_claimed; reaper exchanges it only after
seeing fiber_entered==0; loser bails via xtc_pg_backend_fiber_exit without
touching the slot.  After the fix: cassert smoke PASSes "autovac churn fast stop
clean", 0 TRAP/assert, 0 cores; non-cassert 20/20; process mode unaffected.

REMAINING cassert-only smoke FAILs (benign, timing): under cassert on this
meson-on-btrfs host a few strict count/timing steps ("spawn/exit mismatch",
"fast stop hung", "error unwind / recovery") trip because cassert makes fibers
slower, not because of a fault -- 0 cores, 0 asserts, all subtests reach
"database system is shut down", and the SAME steps PASS in the non-cassert smoke
(same caveat class as the 001_threaded_runtime background_psql harness note).
Use cassert threaded runs to hunt asserts, not to gate on the timing counts.

### Tier C: B_ARCHIVER ADMITTED (2026-07-08, commit 9ad3770f7c2)

While Tier B waits on libxtc, admitted the WAL archiver -- it is NOT blocked by
the fiber+AIO wake-miss.  The archiver's work is file-level (archive_command /
archive library on completed WAL segments), so it never waits on an
io_method=worker shared-buffer completion.  It is a PM_RUN singleton (after
carriers exist) that parks on a bounded-timeout WaitLatch and drains via
ProcessPgArchInterrupts() -> PgCurrentBackendApplyInterrupts().  Its special
two-step shutdown was already wired in thread_child_signal_interrupt: SIGTERM ->
SHUTDOWN_REQUEST (drain, do not die immediately), SIGUSR2 -> WAKEUP_STOP (one
final cycle then proc_exit(0)), SIGINT ignored, SIGQUIT -> PROC_DIE -- symmetric
with process mode's pqsignal handlers.  Validated (archive_mode=on, cp
archive_command): launches as a fiber and archives WAL
(pg_stat_archiver.archived_count=4, 4 files); fast stop clean (0 cores);
immediate stop clean (0 cores); smoke 20/20.  Fiber-eligible set is now: client
backends, bgworkers, autovacuum launcher+worker, WAL writer, WAL summarizer,
archiver.

FOLLOW-UP (commit b514b53759f): the initial Tier C admission left the
archiver's IMMEDIATE-stop path incomplete.  A stricter smoke gate (step 10,
added same session) that watches for SIGKILL escalation caught it: on immediate
stop (SIGQUIT -> PROC_DIE interrupt) the archiver fiber stayed parked in its
WaitLatch and the postmaster had to SIGKILL it (immediate stop returned
non-zero; 0 cores).  ProcessPgArchInterrupts drained the mailbox but never acted
on ProcDiePending.  Fixed by honoring ProcDiePending -> proc_exit(1) at the top
of ProcessPgArchInterrupts, mirroring ProcessWalSummarizerInterrupts.  After the
fix all four archiver smoke gates pass (fiber, archived, fast stop clean,
immediate stop clean with 0 SIGKILL escalations).  Lesson: the fast-stop gate
alone is not enough for a fiber worker with a custom shutdown protocol -- gate
both fast AND immediate stop, and watch for SIGKILL escalation, not just cores.

## libxtc v1.4.2 adopted; walsender teardown fixed; Tier B deferred (2026-07-08)

Bumped `flake.lock` to libxtc **v1.4.2** (rev cb186e3; commit 667489f0b13).
Release lands the fixes for three things we reported/worked around:
- Cross-thread wake of an IDLE loop (lost wakeup) -- the root cause behind our
  autovac orphan-reaper workaround.  Confirmed effect: autovac worker-start
  "took too long" cancels dropped from ~1/run to ~0 (the reaper is now
  defense-in-depth), AND the standby client-backend reaping that stuck the first
  Tier B attempt is FIXED (20 sequential standby conns settle to 1 backend, was
  11 stuck).
- Runtime threads created with signals blocked (__os_pthread_create_masked) --
  overlaps our xtc_pg_carrier_start signal-block fix.
- Allocator discipline + a new public alloc API (additive).
Headers we include are API-unchanged since 1.3.0, so the bump is flake-only.
Smoke 20/20; genuine-crash escalation fires; autovac churn 5/5 clean.
libxtc KNOWN_ISSUES to watch: xtc_exec_fini cross-thread-spawn teardown leak;
one remaining signal-mask integration case.

**Walsender teardown double-free FIXED** (commit b63027eed02): fast-stopping a
primary with a live walsender (a connected standby / any pg_receivewal) crashed
with a glibc heap abort in PgBackendResetWalSenderClosedState.  The walsender
per-backend cells (output/reply/tmpbuf StringInfos + the physical xlogreader)
are allocated in replication_cmd_context, which exec_replication_command resets
per command -- so by backend exit those chunks are already freed and the cells
dangle; the reset pfree'd them again.  Fix: delete replication_cmd_context as
the sole owner-level free, then just clear the dangling cells (lag_tracker,
verified TopMemoryContext-owned, stays a real pfree).  Live-walsender fast stop
3/3 clean.  Pre-existing (reproduced without any widening).

**Tier B (WAL_RECEIVER, SLOTSYNC_WORKER) DEFERRED** (commit 1f2372bcbf8).  With
v1.4.2 + the walsender fix, both families LAUNCH and run as fibers (walreceiver
streams WAL, slotsync starts), but neither shuts down cleanly on a standby:
  - WAL_RECEIVER: standby fast stop -- the walreceiver fiber gets the terminate
    and exits (FATAL "terminating walreceiver process", fiber exit code=256) but
    the postmaster stays in PM_WAIT_* (0 cores -- a wedge).  Standby
    shutdown-ordering between the fiber walreceiver and the thread-carrier
    startup/recovery process.
  - SLOTSYNC_WORKER: launches, but a misconfigured slotsync error-loops
    (relaunch every ~60s) and fast stop does not converge on that churn.
NEXT: fix standby aux-worker fiber shutdown convergence (startup/recovery +
walreceiver ordering; worker relaunch-churn at shutdown), then re-admit Tier B.

## libxtc v1.3.0 adopted (2026-07-06)

Bumped `flake.lock` to libxtc v1.3.0 and took the two API improvements the
libxtc team shipped in reply to our v1.2.1 report (`/tmp/libxtc-reply-2026-07-06.txt`):

1. **Self-describing DOWN** (`xtc_down_decode_ex` -> `xtc_down_info_t`).  The
   supervisor classifies on `di.kind` (CLEAN / EXIT / SIGNAL / NOPROC) and reads
   `di.signal` / `di.exit_code` from separate fields -- no more range heuristic
   over a single `reason` integer, and no dependence on our proc_exit `<< 8`
   convention.  A bare `xtc_exit_self(1)` is now unambiguously KIND_EXIT, not a
   signal-1 fault.
2. **Atomic spawn+monitor** (`xtc_proc_spawn_monitor`).  Replaces the two-step
   `xtc_proc_spawn` + `xtc_monitor`; the monitor is established before the child
   is runnable, so the NOPROC monitor-race case cannot occur and the
   fault-injection test is deterministic.

Also picks up v1.3.0's cross-thread `xtc_send` `wake_revents` atomicity fix,
which is on our exact N-loop backend wake path (now TSan-clean per the libxtc
team).

Verified on the 8-loop pool (floki):
  - smoke 12/12.
  - genuine-crash escalation: `PG_XTC_INJECT_CRASH=2` -> DOWN kind=SIGNAL
    signal=11 -> "terminating threaded server runtime after backend fiber
    crash" -> postmaster DOWN.
  - clean 5-backend run: 6 normal (CLEAN) DOWNs, 0 genuine, 0 non-zero-exit,
    **0 NOPROC**, 0 escalation, fast stop clean.  NOPROC eliminated by
    construction, exactly as the atomic spawn_monitor promised.

Commit: `xtc-carrier: adopt libxtc v1.3.0 -- self-describing DOWN + atomic
spawn_monitor`.

### Item #5 (bgworker fibers) validated on v1.3.0

`B_BG_WORKER` is fiber-eligible and its crash lifecycle is validated:
  - `002_threaded_bgworker_crash` TAP: **6/6 PASS**.  A fiber-backed bgworker
    that `proc_exit(17)`s is observed by the supervisor as KIND_EXIT (not
    escalated there), and the postmaster's own child-crash policy escalates
    ("terminating threaded server runtime after child crash" -> ExitPostmaster).
    A genuine fiber SIGSEGV instead routes through the supervisor's KIND_SIGNAL
    path -- both are correct.
  - Directly exercised and working: 6 concurrent long-lived parked sessions
    (distinct pids), query-cancel of a sleeping fiber ("canceling statement due
    to user request", fiber woke), and idle-backend terminate.

### #5 Tier A widening: B_WAL_WRITER ADMITTED, B_WAL_SUMMARIZER DEFERRED (2026-07-07)

Widened `xtc_carrier_eligible()` (launch_backend.c) to the two Tier A families
from the read-only family audit.  Result: **B_WAL_WRITER is fiber-eligible and
fully validated; B_WAL_SUMMARIZER wedges shutdown and is BACKED OUT** (its
eligibility reverted).  Both already return PG_BACKEND_LAUNCH_THREAD from
PgRuntimeShouldThreadBackend and are launched at PM_RUN (not in the
logger/checkpointer/bgwriter early-process block), so the seam is a one-line
eligibility switch; the difference is purely their wake behavior on the carrier.

**B_WAL_WRITER -- ADMITTED, VALIDATED.**  Launched as an xtc fiber
(pg_stat_activity shows one `walwriter`, pm.log has `xtc: walwriter launched as
xtc fiber`, and the postmaster has ZERO child OS processes -- everything is a
thread/fiber in its address space).  Evidence, on the 8-loop pool (floki),
disk-backed /scratch:
  - runs-as-fiber + real WAL write load (100k-row INSERTs) + SIGHUP reload +
    clean shutdown: fast stop 5/5 CLEAN, immediate stop 3/3 CLEAN, 0 cores, and
    a fresh clean-binary re-run added fast 3/3 + immediate 2/2 CLEAN.
  - robust even after 40s of FULL idle (all loops quiescent) -> fast stop still
    CLEAN.  This is the key property: the WAL writer's parked WaitLatch is woken
    on SIGTERM because committing backends already set its procLatch
    (`xlog.c:2672 SetLatch(walwriterProc->procLatch)`), a self-pipe/signalfd fd
    write that reliably wakes an idle io_uring carrier loop.
  - smoke: added guarded step 8 ("walwriter runs as a fiber and shuts down
    clean": runs-as-fiber + WAL load + clean fast stop, own cluster); smoke now
    16/16 (13 original + 3 new).  The fiber-accounting heuristic (step) also
    counts `walwriter launched as xtc fiber` as a persistent-fiber source
    alongside bgworker lines (the walwriter is a long-lived singleton whose
    best-effort "backend fiber exiting" raw-STDERR write can be lost at the very
    end of shutdown; clean stop proves its pooled-logical exit was published).

**B_WAL_SUMMARIZER -- DEFERRED (shutdown wedge; NOT caused by the fiber swap).**
With `wal_level=replica` + `summarize_wal=on`, a fiber-backed summarizer parks
once at end of startup WAL in `summarizer_wait_for_wal()`
(`WaitLatch(MyLatch, WL_LATCH_SET|WL_TIMEOUT|WL_EXIT_ON_PM_DEATH, up to
MAX_SLEEP_QUANTA*MS = 30s)`) and NEVER advances `summarized_lsn` as WAL is
generated; fast stop then HANGS (`server does not shut down`).
  - Core-dump proof (SIGABRT of the wedged postmaster): NO thread runs
    WalSummarizerMain or WalWriterMain -- both are parked fibers with no
    OS-thread stack; every carrier loop is idle in `xtc_io_poll` /
    `_io_uring_get_cqe`; the scheduler thread is in `clock_nanosleep`; the
    postmaster is stuck in ServerLoop after logging "received fast shutdown
    request" + "aborting any active transactions" (PM_WAIT_*).
  - Root cause: unlike the WAL writer, the summarizer has NO fd-based wake
    source -- nothing sets its latch on new WAL; it relies purely on its own
    WaitLatch TIMEOUT to poll `GetFlushRecPtr()`.  On the xtc carrier that
    bare timer does not fire for a fiber left alone on an otherwise-quiescent
    io_uring loop (the SAME libxtc idle-loop wake gap documented for autovac:
    fd/self-pipe wakes reach an idle loop, a bare timer/mailbox wake does not).
    So it never re-checks WAL, and the same missed wake means the shutdown
    SIGTERM->SHUTDOWN_REQUEST SetLatch cannot rouse it either.
  - **This is a PRE-EXISTING threaded-runtime bug, not a fiber regression.**
    Disproved the fiber hypothesis by re-test after reverting eligibility (so
    the summarizer runs as a THREAD carrier, `walsummarizer launched as xtc
    fiber` = 0): the summarizer STILL fails to advance `summarized_lsn` AND
    fast stop STILL HANGS (core shows no WalSummarizerMain thread; postmaster
    wedged the same way at "aborting any active transactions").  A PROCESS-mode
    control (`multithreaded=off`) is CLEAN: `summarized_lsn` also stays flat in
    the observation window (a measurement/boundary artifact, not a defect) but
    fast stop completes.  So the wedge is specific to the THREADED summarizer
    (thread carrier or fiber), independent of xtc.
  - Backed out: B_WAL_SUMMARIZER stays a thread carrier
    (PgRuntimeShouldThreadBackend); eligibility gate documents the diagnosis.
    Re-admitting it needs either a libxtc timer-wakes-idle-loop fix or an
    fd-based periodic wake for the summarizer -- AND the pre-existing threaded
    summarizer shutdown wedge fixed first (it blocks the thread-carrier path
    too).  Deferral, not regression.

### 001_threaded_runtime harness caveat (pre-existing, NOT a runtime bug)

`001_threaded_runtime` hangs at its `background_psql` section (the 3rd of five
long-lived interactive sessions) in THIS meson-on-btrfs environment, timing out
on IPC::Run.  It is an IPC::Run/`background_psql` harness interaction, not a
runtime defect: BISECTED to fail IDENTICALLY on libxtc **v1.2.1 and v1.3.0**
(reverted the carrier + flake to 1.2.1, rebuilt, same "exited just after 12"
timeout), and the behaviors it means to test all pass when driven directly
(6 concurrent parked sessions, cancel, terminate -- see above).  Per the
standing note, run full 001 under `gmake check-threaded` on a disk-backed host;
the meson-only path is not the supported way to run it.

### #5 next family: autovacuum TRIED as fibers, BACKED OUT (2026-07-06)

Widened `xtc_carrier_eligible()` to admit `B_AUTOVAC_LAUNCHER` +
`B_AUTOVAC_WORKER` as fibers, then backed it out after finding a shutdown hang.
Evidence:
  - Idle launcher (naptime 3600, no workers): fast stop CLEAN.  The shutdown
    interrupt's cross-fiber SetLatch unparks the long-nap launcher fiber
    correctly -- so the launcher itself is fine as a fiber.
  - Under churn (naptime 1, threshold 1, cost_delay 0): fast stop HUNG,
    reproducibly, `spawned=20 exited=18` (2 fibers un-reaped), always
    correlated with one `WARNING: autovacuum worker took too long to start;
    canceled`.
  - Root cause: the `autovacuum_worker_start_timeout` cancel path races the
    fiber launch.  A worker whose fiber is slow to reach the launcher
    start-handshake gets canceled by the launcher; the already-spawned fiber
    is orphaned (its PMChild slot is torn down by the cancel) and never
    reaped through the pooled-logical exit path, so PM_WAIT_BACKENDS wedges at
    fast stop.
  - Backed out: autovac stays a THREAD carrier (PgRuntimeShouldThreadBackend),
    where churn+shutdown is CLEAN and 001 subtests 6-8 already cover it.
    Verified after revert: 0 autovac worker fibers, churn+fast-stop CLEAN,
    smoke 12/12.

Follow-up to re-admit autovac as fibers: make the worker-start-timeout cancel
fiber-aware -- reconcile the canceled PMChild slot with the orphaned worker
fiber's DOWN (the supervisor already observes it), so a canceled-but-spawned
worker is reaped rather than left parked.  This is a focused autovac-lifecycle
fix, tracked under #5; not a libxtc issue.

Sharper diagnosis (2026-07-06, re-instrumented with a FIBER ENTER trace at
carrier-proc entry, debug2 postmaster log):
  - Both stuck fibers ENTER their body (so it is not a launch-stall); the two
    that show `spawned` without a matching `backend fiber exiting` are the
    long-lived launchers (autovac launcher child_slot=221, logrep launcher
    child_slot=238), spawned first.
  - BUT the postmaster debug2 log shows BOTH launchers DO reach exit at
    shutdown ("autovacuum launcher (PID 0) exited with exit code 0",
    "background worker ... exited", their pm child slots 221/238 released).
    So the launchers are not the wedge; my "backend fiber exiting" raw-write
    just is not on the launcher proc_exit path.
  - After reaping both launchers the postmaster STALLS in PM_WAIT_BACKENDS with
    every carrier loop idle (all threads in io_cqring_wait / do_epoll_wait, no
    runnable fiber, no pending timer).  So it is waiting on a target-mask child
    (B_BACKEND / B_AUTOVAC_WORKER / B_BG_WORKER) whose PMChild exit is never
    published -- i.e. an on-demand autovac WORKER fiber that was launched, ran,
    and should have exited but whose pooled-logical exit did not reach
    process_pm_pooled_logical_exit().
  - The trigger is intermittent and correlates with the worker-start-timeout
    cancel window: when the launcher cancels a slow-to-start worker slot, the
    already-spawned worker fiber and the canceled PMChild/WorkerInfo bookkeeping
    can diverge so the fiber's exit is not matched to a waited-on slot.
  So the fix is specifically: on the fiber path, guarantee that a
  canceled-but-spawned autovac worker's fiber exit is published as a
  pooled-logical exit for its PMChild slot (or that the cancel tears the slot
  down through the same reaper), so PM_WAIT_BACKENDS cannot wait on a ghost.
  This is worth a dedicated, carefully-tested change (it touches the
  worker-start-timeout cancel + PMChild reaping race) rather than an in-session
  patch; autovac-as-thread-carrier remains the correct shipping state until
  then.  It likely also covers other on-demand worker families with a
  start-timeout cancel.

### ROOT CAUSE (2026-07-06, deeper): concurrent GUC SET/RESET wedges the fiber runtime

Further bisection (raw-write markers through backend_thread_entry, since elog
is unsafe pre-error-stack there) reframes the autovac hang as a symptom of a
BROADER pre-existing bug, independent of autovac:

  - Direct repro on plain HEAD (no autovac): open 8 concurrent client backends,
    each running a `set_config()/RESET work_mem` loop.  One session wedges
    (rc=124 timeout) and fast stop HANGS.  A control of 8 concurrent backends
    doing plain work (no SET/RESET) is completely clean.  So the trigger is
    concurrent GUC SET/RESET across fiber backends, not concurrency itself.
  - Mechanism: guc.c ThreadedGUCLock() takes a process-wide `pthread_mutex_t`
    (ThreadedGUCMutex).  That is a LOOP-BLOCKING primitive under the fiber
    carrier: if fiber A on a carrier loop holds it and yields, and fiber B on
    the SAME loop then calls pthread_mutex_lock, B blocks the OS thread, so A
    can never be resumed to release it -- the loop deadlocks.  The autovac
    worker burst hit this because rapid workers land >1 fiber per loop, both in
    InitializeThreadedSessionGUCOptions() (which takes ThreadedGUCLock); the
    SET/RESET storm hits the identical lock directly and reproduces it without
    autovac.
  - The autovac "canceled-but-spawned worker not reaped" story from the section
    above is a downstream effect: the loop is already wedged on the GUC mutex,
    so the canceled worker's fiber can never run to publish its exit.

Attempted fix (BACKED OUT -- regression): route ThreadedGUCLock through an
`xtc_amutex` (fiber-parking mutex, xtc_sync.h) under USE_XTC_CARRIER via
xtc_pg_guc_lock/unlock wrappers.  It did NOT fix the wedge and made the storm
WORSE (sessions failed rc=2 instead of one timing out).  Likely causes to work
through before retrying: the HOLD_INTERRUPTS bracketing across a parking lock;
xtc_amutex_static recursion vs. guc.c's own per-fiber ThreadedGUCMutexDepth
double-counting; and the on-loop-park vs off-loop-condvar hand-off when a
pthread-carrier io worker and a fiber backend contend the same amutex.  Reverted
to HEAD (pthread mutex); tree stays clean, smoke 12/12.

This is the REAL #5 blocker and it is broader than autovac: it blocks any
concurrent GUC SET/RESET (and any two fibers contending ThreadedGUCLock) on the
xtc carrier.  Fixing it wants a dedicated change with a concurrent-SET/RESET
regression test as the gate, and a correct fiber-aware lock that (a) never
blocks the loop OS thread while a sibling fiber holds it, (b) preserves the
interrupt-hold + recursion contract, and (c) is one lock object shared
correctly between on-loop fibers and off-loop pthread carriers.  Until then,
autovac (and any fiber worker family that does contended GUC work) stays on
thread carriers.

### RESOLVED (2026-07-06) -- and the loop-blocking-mutex diagnosis above was WRONG

The wedge is fixed, but NOT by touching the lock primitive.  A/B test (v1.4.0,
everything else identical, only ThreadedGUCUnlock swapped) is decisive:
  - buggy ThreadedGUCUnlock  -> 16-session SET/RESET storm leaves POST WEDGED +
    fast stop HUNG.
  - fixed  ThreadedGUCUnlock  -> POST OK + stop CLEAN across many runs
    (16 sessions x 400 iters, repeated).
So the real cause was the ThreadedGUCUnlock ACCOUNTING bug (commit
639323786c6), not the pthread mutex: ThreadedGUCUnlock re-read `multithreaded`,
which the `multithreaded` GUC set itself flips between the paired lock/unlock,
so on the off->on straddle it ran an UNBALANCED RESUME_INTERRUPTS and
underflowed the per-carrier depth -- corrupting interrupt state process-wide and
wedging the runtime under concurrent SET/RESET.

The "loop-blocking pthread_mutex" hypothesis was a MISDIAGNOSIS: the threaded
GUC critical section (set_config_with_handle_internal, build_guc_variables,
etc.) is pure in-memory GUC-table work and NEVER yields/parks the fiber while
holding ThreadedGUCMutex.  A fiber therefore always runs from lock to unlock
without the loop switching to a sibling, so same-loop fiber contention on the
mutex cannot occur; cross-loop (real OS thread) contention is brief and correct.
No fiber-aware lock is needed.  The earlier xtc_amutex attempt was both
unnecessary and (as its regression showed) wrong; it stays reverted.

Gate: smoke step 2b ("concurrent GUC SET/RESET (no wedge)") -- 8 concurrent
set_config/RESET loops then a fresh query; passes with the fix, wedges without.
#5's GUC-lock hazard is CLOSED.

CORRECTION (2026-07-06, later): I initially claimed this GUC-unlock fix also
unblocked autovac-as-fibers.  That was WRONG -- disproven by re-test: with
autovac eligibility re-enabled on top of the GUC fix, the churn+fast-stop
scenario STILL HUNG deterministically (3/3 runs), always with one "autovacuum
 worker took too long to start; canceled".  So the autovac hang is a SEPARATE,
real race, not the GUC-unlock bug.  Autovac stays deferred; see the dedicated
section below.

### Autovac fiber-widening: the REAL blocker (worker-start-timeout cancel/reap race)  [FIXED 2026-07-06]

RESOLVED and autovac is now fiber-eligible (commit 701ac3db844), with the
threaded start/shutdown-ordering prerequisites in 67c604136e3.  Sharper root
cause than first written: the canceled worker fiber NEVER RUNS -- a libxtc
cross-thread wake miss to an idle io_uring carrier loop (a fiber spawned onto a
quiescent loop is never scheduled; fd/self-pipe wakes reach idle loops, a
mailbox/spawn wake does not).  The postmaster had optimistically published the
worker PMChild; the launcher start-timeout cancel reclaims the shmem WorkerInfo
but nothing reconciles the orphaned PMChild -> PM_WAIT_BACKENDS wedges.  Fix:
the postmaster reaps the orphan (ReapOrphanedThreadedWorker), triggered by a
new PMSIGNAL_AUTOVAC_WORKER_TIMEOUT from the launcher cancel (age-gated) and by
a PM_WAIT_BACKENDS drain; a fiber_entered/exit_claimed guard makes it
exactly-once and never grabs a live/relaunched worker.  Validated: churn +
fast stop CLEAN 5/5 and 10/10 (non-cassert), process mode unchanged, smoke
13/13.  The underlying libxtc idle-loop cross-thread-wake miss is unfixed and
is a libxtc item to REPORT; the PG fix makes us robust to it.

Also fixed in the same effort (67c604136e3), surfaced once aux/autovac workers
run as fibers under cassert (all base-tree ordering bugs, reproduce with xtc
disabled): checkpointer process->thread handoff wrongly flipping the shared
pgstat is_shutdown flag; deferred InitXLogInsert creating the WAL-record ctx
inside a critical section for aux/maintenance workers; and pthread_create not
blocking signals across thread creation (SIGCHLD -> Assert(MyProcPid)).

KNOWN FOLLOW-UP: FIXED (2026-07-06).  The intermittent aset-freelist / analyze
double-free when a worker proc_exit(1)s mid-fast-stop turned out to be TWO bugs:
(1) InitXLogInsert deferral was unsound even for B_BACKEND (first WAL insert can
be in a crit section -- heap_update lock-old-tuple, etc.); now eager for all
backends (commit 3d834e9993b).  (2) the persistent PgExecution analyze_context
cell dangled when a FATAL unwound out of do_analyze_rel (transaction abort frees
the context tree, cell left pointing at freed memory, closed-state reset deletes
it again); fixed with a context reset callback that NULLs the cell (commit
7a37c8d2279).  Verified: cassert autovac churn + fast stop 10/10 CLEAN, 0 cores,
0 TRAP; non-cassert smoke 13/13.

### Threaded multi-writer / concurrent-fiber shared-state bugs (2026-07-06, agent team)

A team of agents widened B_WAL_WRITER to fibers (Tier A) and fixed the
concurrent-WAL-insert race, which surfaced a cluster of related
shared-process-state bugs on the multi-writer / concurrent-fiber path.  All are
base-tree session-runtime issues (a former per-process static/global now shared
across fibers), all fixed this session:
  - FIXED: GetXLogBuffer() one-entry WAL-page cache was two function-local
    statics -> shared across fibers -> Assert(xlp_pageaddr) at xlog.c:1683 with
    6+ writers.  Moved to the per-backend PgExecutionXLogInsertState bucket
    (commit 96d94cc2f91).
  - FIXED: libxtc carrier threads (scheduler + loop/worker) were born with the
    postmaster`s UNBLOCKED signal mask (bare pthread_create in libxtc), so a
    process-directed SIGCHLD hit an idle carrier/scheduler thread where
    MyProcPid==0 -> Assert(MyProcPid) in wrapper_handler.  Block signals across
    xtc bringup (commit a0a7d1c4cdb).
  - FIXED: concurrent fiber process-title updates raced the one-per-process
    ps_buffer length bookkeeping -> Assert(strlen(ps_buffer)==ps_buffer_cur_len).
    Serialized set_ps_display*/get_ps_display with a multithreaded-gated mutex
    (commit f069b490fc5).
  - FIXED (prior, same session): the InitXLogInsert-in-crit-section and
    checkpointer-handoff pgstat is_shutdown and terminated-worker analyze_context
    double-free bugs.

STILL OPEN (pre-existing, observed -- next-session candidates):
  - FIXED (2026-07-07): Assert(PostmasterChildIsThread(pmchild)) at
    pmchild.c:577 on the fiber-backed ApplyLauncher exit during immediate stop.
    Root cause: backend_thread_finish chose the exit-publish routine from the
    per-OS-thread xtc_in_backend_fiber flag, which a SIBLING fiber on the same
    loop clears while the ApplyLauncher is parked -> stale-false -> wrong
    (thread) publish.  Fixed by keying off the durable PMChild carrier_kind
    (PostmasterChildIsPooledLogical), commit 96335a703db.  A companion reap-side
    bug -- process_pm_pooled_logical_exit reaping fiber aux workers via bare
    CleanupBackend, leaving WalWriterPMChild dangling (Assert(WalWriterPMChild
    == NULL) at PM_WAIT_DEAD_END) -- is fixed in a5d38a83964 (shared
    reap_aux_or_backend_child dispatch).
  - FIXED (2026-07-07): Threaded WAL SUMMARIZER wedge.  The earlier diagnosis
    (a bare-timer WaitLatch that never fires for a lone backend / libxtc
    idle-loop wake gap) was WRONG.  The real cause was that the threaded
    summarizer had no PROC_DIE handling, so an immediate-stop SIGQUIT (delivered
    as a PROC_DIE interrupt in threaded mode, not a crash-exit) left it parked;
    ProcessWalSummarizerInterrupts now honors ProcDiePending (commit
    b710a12e629).  The "never advances summarized_lsn" was a MISREAD: summarized
    _lsn is stable in process mode too across the same small workload; the
    summarizer produces summaries in both modes.  B_WAL_SUMMARIZER is now
    fiber-eligible; summaries produced, fast + immediate stop clean, smoke 20/20.
  - A concurrent-WAL-insert Assert(xlp_pageaddr) at xlog.c with 6+ parallel
    writers is FIXED; a further xlog.c concurrency assert may exist at much
    higher writer counts -- re-check under heavy multi-writer load.

#### Historical diagnosis (kept for reference)

Instrumented the worker fiber lifecycle end to end (raw-write markers, since
elog is unsafe pre-error-stack).  Findings on the churn+fast-stop hang:
  - Every spawned fiber runs its body (xtc_carrier_proc "fiber body running"
    count == supervisor "spawned" count), so it is NOT a libxtc
    spawned-but-never-scheduled problem.
  - 2 fibers run their body but never reach "backend fiber exiting"; the STUCK
    POINT VARIES run-to-run -- sometimes before pre-GUCopts (early
    backend_thread_entry init), sometimes past it -- i.e. it is timing
    dependent, not a fixed deadlock.
  - The hang always correlates with exactly one
    `autovacuum_worker_took_too_long_to_start; canceled`.  The postmaster then
    stalls in PM_WAIT_BACKENDS with all loops idle, waiting on a worker PMChild
    whose pooled-logical exit is never published.

Root cause: a race between the launcher's autovacuum_worker_start_timeout
cancel and the worker FIBER's lifecycle.  Process-mode assumes a canceled
worker is a forked process the OS reaps via SIGCHLD (the worker eventually runs,
finds av_startingWorker == NULL, proc_exit(0)s, postmaster reaps).  For a
POOLED-LOGICAL FIBER that assumption breaks: fiber scheduling latency on a busy
loop pool sometimes exceeds the start timeout, the launcher cancels + reclaims
the shmem WorkerInfo, and the orphaned worker fiber's PMChild is never
reconciled with a published exit -- so PM_WAIT_BACKENDS waits forever.

Core-dump proof (SIGABRT of the wedged postmaster): pmState == PM_WAIT_BACKENDS,
Shutdown == fast, and ActiveChildList still holds exactly ONE B_AUTOVAC_WORKER
(bkend_type=4, pid=0 -> pooled-logical fiber) plus the process-backed
checkpointer/bgwriter (reaped in a later state).  The un-reaped worker fiber is
parked in WaitLatch and the postmaster's fast-stop interrupt (SendInterrupt ->
SetLatch(interrupt_latch)) never drives it to publish its pooled-logical exit.
Exactly one such worker per hang, 1:1 with one "took too long to start;
canceled".

Why this is NOT a quick unblock: the fix must make the worker-start-timeout
cancel fiber-aware -- guarantee that a canceled-but-spawned worker fiber's
PMChild is always reaped: either (a) the launcher-cancel path drives the
orphaned slot through process_pm_pooled_logical_exit, or (b) a canceled worker
fiber deterministically reaches a proc_exit that publishes its pooled-logical
exit AND the fast-stop interrupt reliably wakes it out of WaitLatch.  That is a
dedicated postmaster/autovac lifecycle change with its own churn+fast-stop TAP
gate; it touches the launcher cancel + PMChild reaping handshake.  Autovac runs
correctly as a THREAD carrier meanwhile (PgRuntimeShouldThreadBackend), so this
stays deferral, not regression.  Eligibility gate reverted (autovac not
fiber-eligible) until this lands.

---

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

5. **cassert build.**  CLOSED (2026-07-06).  Under `--enable-cassert`,
   bootstrap-mode `initdb` aborted before reaching BKI.  All three aborts are
   BASE-TREE bugs in the session-runtime state accessors, reproduced with the
   xtc carrier DISABLED (`-Dxtc=disabled`) -- not xtc.  ALL THREE FIXED; cassert
   initdb now Succeeds end to end.  Full write-up in
   `/tmp/pg-bootstrap-cassert-bugs.md`.
     - FIXED #1: `guc.c:1612 Assert(GUCMemoryContext == NULL)` tripped on its
       own read side effect -- the `GUCMemoryContext` macro is
       `*PgCurrentGUCMemoryContextRef()`, whose accessor lazily CREATES the
       early-fallback context when no session is installed.  Fix: non-allocating
       `PgCurrentGUCMemoryContextPeek()` used by the assert.
     - FIXED #2: `backend_runtime_backend.c:601`
       `PgBackendAdoptEarlyMemoryManagerState()` asserted the early aset.c
       `context_freelists` were empty -- a false invariant (AllocSetDelete
       caches freelist headers; bootstrap's config/tz processing legitimately
       leaves them non-empty).  Fix: drop the emptiness asserts.
     - FIXED #3: `fd.c:1300 Assert(numExternalFDs > 0)` in `ReleaseExternalFD`
       via `FreeWaitEventSet <- PgBackendResetIPCClosedState <-
       PgBackendResetClosedState <- PgBackendExit`.  State-straddle confirmed by
       a reserve/release ledger: `num_external_fds` counts FDs owned by the
       WaitEventSets (latch set, self-pipe, signalfd, FeBeWaitSet), released
       LATER in the exit path.  The `storage` bucket reset runs BEFORE the `ipc`
       bucket reset and zeroed the counter twice
       (`PgBackendResetFileAccessClosedState` `= 0` + the
       `PgBackendInitializeStorageState` MemSet), so the still-pending releases
       underflowed.  Fix: preserve `num_external_fds` across
       `PgBackendResetStorageClosedState` so the explicit releases drive it to 0
       (commit `894fee47e99`).
     - FIXED #4: `fd.c:1300 Assert(numExternalFDs > 0)` again, but in a FORKED
       CHILD at startup via `FreeWaitEventSetAfterFork <- ClosePostmasterPorts
       <- postmaster_child_launch` (exposed once #3 let initdb finish and a full
       cluster could start).  A forked child inherits the postmaster's open
       external FDs (death-watch pipe, pm_wait_set epoll) and releases them in
       ClosePostmasterPorts, but `fork_process()` ->
       `PgRuntimeResetAfterFork()` -> `PgBackendResetEarlyFallbackAfterFork()`
       zeroed the inherited `num_external_fds` (ledger: same cell, val 4->0
       across the fork), so those first releases underflowed.  Upstream's
       process-global counter survived fork; the per-state cell does not.  Fix:
       preserve `num_external_fds` across the fork fallback reset (commit
       `3bcee4eff42`).
   Non-cassert `initdb` Succeeds and the xtc smoke stays 11/11 after all four.
   The xtc carrier can now be built/run under `--enable-cassert`, and a full
   cassert cluster (process mode) does initdb + start + query + fast stop with
   ZERO TRAP/PANIC.  (Separate, still open: the StartupProcess proc_exit ->
   PgBackendResetXLogClosedState -> MemoryContextDelete teardown SIGSEGV, same
   session-runtime teardown family, item #2.)

6. **Remove diagnostic writes.**  CLOSED (2026-07-06).  Dropped the two
   proof-of-life raw writes named in this item -- "backend fiber entered" (one
   per fiber) and "fiber wait_fd ..." (one per park, the highest-volume line).
   Kept the structured, rate-limited lifecycle writes (supervisor DOWN
   classifications, "spawned backend fiber", "backend fiber exiting", and the
   guarded PG_XTC_INJECT_CRASH proof); the smoke greps the spawn/exit/worker
   lines for its fiber accounting, and elog is still unsafe from the bare
   pre-PG-init carrier fiber.  Smoke still 12/12.

## libxtc notes

No libxtc bug found in the reporting session -- v1.2.1 behaved correctly
(cross-fiber wakeup, ERROR unwind, DOWN contract, containment + escalation all
validated).  We raised one feature observation (the v1.2.1 DOWN `reason` integer
overloaded signal-number and app-exit-status namespaces; a bare
`xtc_exit_self(1)` was indistinguishable from a signal-1 fault -- we only avoided
it because PG exit codes arrive pre-shifted `<< 8`) and one docs note
(fault-guard install is per-loop-thread and required for containment), recorded
in `/tmp/libxtc-notes.md`.

RESOLVED in libxtc v1.3.0 (reply in `/tmp/libxtc-reply-2026-07-06.txt`):
  - the feature request landed as our preferred Option 1 -- the self-describing
    `xtc_down_decode_ex` / `xtc_down_info_t` (kind + separate signal/exit_code);
  - the docs note is now in the `xtc_proc.3` man page;
  - they additionally shipped atomic `xtc_proc_spawn_link`/`_spawn_monitor` and
    a cross-thread `xtc_send` wake_revents atomicity fix on our hot path.
We adopted v1.3.0 and both APIs (see the v1.3.0 section above).

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

## Merge: heikki/master catch-up (2026-07-11, commit e010c92fd03)

Caught xtc up onto its base branch heikki/master (+76 commits) via merge (this
branch tracks its base by merge, not a 1198-commit replay rebase).  10 files
conflicted; resolutions:

- src/include/optimizer/cost.h, src/backend/optimizer/path/costsize.c: kept
  xtc's per-session-relocated planner GUC accessors (PgCurrent...Ref); re-added
  the NEW upstream GUC enable_groupagg (commit e01b23b84e4) as a plain global
  (not yet relocated -- ponytail-marked for a Phase 16 GUC pass).
- src/backend/storage/lmgr/proc.c, src/include/storage/proc.h: took heikki's
  "Refactor how some aux processes advertise their ProcNumber" (avLauncherProc
  advertised/cleared in InitProcess/ProcKill, not InitAuxiliaryProcess) while
  keeping xtc's threaded backendId set on the aux PGPROC.
- src/backend/postmaster/autovacuum.c: xtc's AutoVacWorkerMain already used the
  new avLauncherProc-based launcher wake (ahead of heikki's change) and had no
  av_launcherpid references, so kept HEAD and dropped the heikki duplicate.
- src/backend/storage/ipc/procsignal.c, src/backend/storage/lmgr/lock.c: kept
  xtc's threaded forms (volatile ProcSignalSlot + PgBackendInterruptType;
  PG_GLOBAL_SHMEM volatile FastPathStrongRelationLocks).
- pgrepack.c, pg_plan_advice.c, pg_stash_advice.c: kept BOTH heikki's .name/
  .version magic fields AND xtc's PG_MODULE_MAGIC_BACKEND_MODEL_THREAD_PER_SESSION.

Validated: a fresh meson build against libxtc v1.12.0 compiles + links the whole
tree clean (970/970, postgres linked).  NB: the long-lived build-mt-v121 dir has
a stale libxtc-1.10.0 dependency cache (3236 refs to a GC'd nix store path) and
needs a clean re-setup; a fresh build dir builds fine.

## Merge: upstream/master catch-up + base switch (2026-07-11, commit 8bf898c824f)

Switched the branch base from heikki/master to plain upstream postgres/master
(they had nearly converged: heikki = upstream + 2 commits, both already in
upstream too).  Brought in 16 upstream commits.  Only 1 file conflicted:

- src/backend/utils/activity/wait_event.c: upstream 8f7af125e03 "Remove
  WaitEventCustomCounterData" (the spinlock was unnecessary -- the counter is
  only touched under WaitEventCustomLock held exclusively).  Took upstream's
  plain `int *WaitEventCustomCounter` but kept xtc's PG_GLOBAL_SHMEM placement;
  dropped the now-unused storage/spin.h include, kept utils/backend_runtime.h.

Auto-merge break fixed: an upstream commit renamed the macro
pg_attribute_always_inline -> pg_always_inline tree-wide.  Upstream renamed its
own uses, but 22 xtc-added uses (trigger.c, pqcomm.c, bufmgr.c, postgres.c,
syscache.c -- the GetCurrent...Data per-session accessors) still referenced the
old name and failed to compile.  Renamed all 22.

Validated: fresh build against libxtc v1.12.0 compiles + links clean (577/577,
postgres linked).

Going forward: the branch tracks upstream/master.  Heikki Linnakangas's
multithreading work is cherry-picked/incorporated as needed during future
upstream rebases, not tracked as the base (his branch stayed ~= upstream, so
there is little unique MT foundation left there to track separately).
