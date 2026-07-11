# AGENTS_XTC.md -- libxtc integration steering (M16: threaded PostgreSQL on xtc)

Companion to AGENTS.md.  This file steers work on the `xtc` branch (the
xtc-carrier integration): running PostgreSQL backends as libxtc fibers
on an xtc scheduler.  It captures the plan, the current state, and the
hard-won DOs / DON'Ts / debugging techniques for working with libxtc.

libxtc is a C11 concurrency runtime (Tokio/Seastar/BEAM-style: work-
stealing executor, stackful fibers, actor procs with mailboxes,
supervisors).  Source tree: point XTC_ROOT at it (default
/home/gburd/ws/xtc, OVERRIDE on your machine).  Its own steering is in
that tree's AGENTS.md + docs/ (esp. docs/M16_PG_ADAPTER.md, PLAN.md,
docs/M_DST.md).


## 1. What this branch is, and what "on xtc" means today

This is Sam Willis's multithreaded PostgreSQL (thread-per-session
backends + a carrier-thread pool) with an xtc CARRIER wired in: a
regular client backend (B_BACKEND) runs as an xtc_proc FIBER on an xtc
scheduler loop instead of on a raw pthread, and its client-socket waits
park cooperatively on the xtc loop via xtc_proc_wait_fd.

The strategy that unlocked this: DO NOT __thread-ize stock PostgreSQL's
process-globals yourself (an earlier fork-replacement spike in
~/ws/postgres/master branch xtc-m16-1b-spike hit exactly that wall).
Start from an ALREADY-THREADED PostgreSQL and change only the CARRIER
(what thread/fiber a backend runs on).  The globals wall is gone because
the base tree already solved it.

### Current state (verified)

- SINGLE backend: `psql -c "select 1"` round-trips through the xtc
  scheduler.  SOLID.
- N SEQUENTIAL backends: connect/query/disconnect x N -- each spawns a
  fresh fiber, returns correct rows, exits cleanly (slot reused with a
  bumped generation, no leak, no hang).  SOLID.
- N CONCURRENT backends: on a CARRIER LOOP POOL (n_loops = CPU count) each
  backend runs on its own loop, all return correct rows, and all exit
  cleanly (spawned == exited); a new query after a concurrent burst is not
  wedged.  SOLID.  The single-loop lost-wakeup that wedged 2+ parked fibers
  is resolved by the pool (see section 2 item 1 and 4).
- SHUTDOWN: `pg_ctl -m fast stop` completes after backends have run.  The
  fiber-backed backend is classified as a pooled-logical PMChild so the
  postmaster reaps it without pthread_join (it has no dedicated joinable
  thread); before this, PM_WAIT_BACKENDS hung on a bogus join.

Only B_BACKEND is on xtc.  The postmaster is a process; B_STARTUP and
early auxiliaries still fork; other auxiliaries (bgworker, io_worker,
post-startup logger/checkpointer/bgwriter) run on the BASE tree's own
pthread carriers, NOT on xtc.  So this is threaded-client-backends-on-
xtc, not a fully fork-free PostgreSQL.

Build + test: use the Nix flake + meson/ninja (`-Dxtc=enabled`).  Run tests
on a disk-backed host (meh/nuc/EC2), NOT on /tmp or other tmpfs -- RAM-backed
scratch dirs get wiped between steps here and make server tests unreliable.


## 2. The M16 plan -- remaining work, roughly in order

Near-term (the xtc carrier itself):

1. CONCURRENT-backend lost-wakeup.  DONE.  Fixed by the carrier loop pool:
   each backend fiber is placed round-robin across `xtc_exec_loop()` so two
   concurrent backends never park on the same loop and cannot starve each
   other.  Also fixed the shutdown hang (pooled-logical fiber reaping).
2. Cross-fiber SetLatch wakeups.  DONE (verified on meh): a parked LISTEN
   session wakes and delivers when a DIFFERENT backend (different fiber, likely
   different loop) does NOTIFY.  The existing signalfd+epoll intercept plus
   SetLatch(backend->interrupt_latch) already handle it -- no XTC_WAIT_MAILBOX
   path was needed.  Covered by scripts/xtc_smoke.sh.
3. ereport(ERROR) / sigsetjmp inside a fiber.  PG's error unwind
   (PG_exception_stack) is per-thread in this tree; on a fiber stack it
   should hold for one backend, but confirm an ERROR unwinds cleanly and
   the fiber survives.  DONE (verified on meh): SELECT 1/0 then a follow-up
   query in the same session -- the error unwinds and the session recovers
   on the fiber stack.  Covered by scripts/xtc_smoke.sh.
4. A real carrier POOL: many xtc loops/threads (the Phase-15 shape).
   DONE for backends -- n_loops = CPU count (override PG_XTC_CARRIER_LOOPS),
   backends placed round-robin across the executor loops.  Still cooperative
   within a loop; work-stealing across loops is xtc's default.

Larger (toward fully-on-xtc):

5. Auxiliary/background processes on xtc: route bgworker, checkpointer,
   walwriter, autovacuum, etc. through the xtc scheduler instead of fork /
   base pthreads.  B_BACKEND + B_BG_WORKER + AUTOVACUUM (launcher+worker) +
   B_WAL_WRITER + B_WAL_SUMMARIZER ADMITTED; other families DEFERRED (ranked
   plan below).
   xtc_carrier_eligible() (launch_backend.c) gates which child types run as
   fibers; the launch/exit plumbing is generalized for any type.  B_BG_WORKER
   is now fiber-eligible (unblocked by #7's fiber-death observation +
   escalation): the earlier idle-terminate/crash wedge is resolved.
   VALIDATED (2026-07-06, 8-loop pool, libxtc v1.3.0): the
   002_threaded_bgworker_crash TAP passes 6/6 -- a fiber-backed bgworker that
   proc_exit(17)s escalates via the postmaster's own child-crash policy
   ("terminating threaded server runtime after child crash" -> ExitPostmaster),
   while a genuine fiber SIGSEGV would route through the supervisor's
   KIND_SIGNAL path; both are correct.  Concurrent long-lived parked sessions
   (6 verified), query-cancel of a sleeping fiber, and terminate all work when
   exercised directly.  Widen the remaining families one at a time, each
   validated on a fiber.
   WIDENING ORDER (from the 2026-07-06 read-only family audit; all 9 are now
   GUC-startup-safe after the ThreadedGUCUnlock fix, and all route waits through
   the xtc intercept + pooled-logical exit):
     - Tier A: B_WAL_WRITER **ADMITTED** (2026-07-07; runs-as-fiber, WAL write
       load, SIGHUP, clean fast+immediate stop incl. 40s-idle).
       B_WAL_SUMMARIZER **ADMITTED** (2026-07-07).  Its earlier shutdown wedge
       was NOT a libxtc timer-wake gap as first hypothesized -- it was two
       teardown bugs, now fixed: (1) the summarizer had no PROC_DIE handling, so
       an immediate-stop SIGQUIT (delivered as a PROC_DIE interrupt in threaded
       mode, not a crash-exit) left it parked -- ProcessWalSummarizerInterrupts
       now honors ProcDiePending; and (2) fiber aux workers were reaped via bare
       CleanupBackend, leaving the family`s global PMChild pointer dangling
       (Assert(WalWriterPMChild == NULL)) -- process_pm_pooled_logical_exit now
       runs the per-type cleanup via the shared reap_aux_or_backend_child().
       Also fixed the fiber-vs-thread exit-publish routing to key off the
       durable PMChild carrier_kind (not the per-OS-thread xtc_in_backend_fiber
       flag, which a sibling fiber can clear).  Validated: summaries produced
       (matches process mode), fast + immediate stop clean, smoke 20/20.
     - Tier B: B_SLOTSYNC_WORKER, B_WAL_RECEIVER -- ADMITTED (2026-07-09,
       commits 42fe3a99469 + 5df28304993).  Root cause of the original defer was
       OUR fork/fiber MIXED model, NOT a libxtc bug: on a hot standby a fresh
       fiber CLIENT backend hung during InitPostgres on its first
       uncached-catalog async read, and a hung backend keeps PM_WAIT_BACKENDS
       from converging (that was the "shutdown wedge").  GDB + /proc: client
       backends run as FIBERS inside the postmaster, but io workers (+
       checkpointer, bgwriter) are still FORKED PROCESSES (launch at PM_STARTUP
       before carriers exist); a forked io worker completing the backend's AIO
       SetLatch()es it, but owner_pid(postmaster) != MyProcPid(io-worker-fork)
       so SetLatch takes the cross-PROCESS kill(SIGURG) path aimed at the
       postmaster, which cannot wake the fiber's carrier loop.  xtc_proc_wake
       cannot cross a process boundary.
       INTERIM FIX (commit 2c9749b2da2): under multithreaded=on, route the
       default io_method=worker to the in-fiber "xtc" method (issuer-synchronous
       READV/WRITEV on the fiber, no foreign completer) by rewriting the GUC
       value after SelectConfigFiles.  With that, standby fiber backends serve
       queries and both Tier B families are validated: walreceiver launches as a
       fiber + streams + standby serves replicated rows; slotsync worker
       launches once (no relaunch churn) + syncs; BOTH fast AND immediate stop
       of standby+primary clean (0 cores, 0 SIGKILL escalations).  Process mode
       (multithreaded=off) byte-for-byte untouched (io_method=worker + forks).
       REAL FIX still owed (Tier D/F): run the completers IN-PROCESS as thread
       carriers via the early-start process->thread HAND-OFF, then io_method=
       worker wakes fibers via the wired xtc_proc_wake (commit dbaf5c9580b) and
       the interim remap can be removed.  NOTHING further needed from libxtc
       (v1.8.0's xtc_proc_wake is the right primitive).  Full analysis:
       /tmp/tier-b-fork-vs-fiber-rootcause.md.  See M16_XTC_CARRIER_FINDINGS.md.
       (v1.4.2 fixed the standby client-backend REAPING; primary
       walsender-teardown double-free is fixed (b63027eed02).)
     - Tier C: B_ARCHIVER -- ADMITTED (2026-07-08, commits 9ad3770f7c2 +
       b514b53759f).  Not blocked by the Tier B fiber+AIO wake-miss: its work
       is file-level (archive_command / archive library on WAL segments), never
       an io_method=worker shared-buffer completion wait.  Started at PM_RUN
       (after carriers).  Shutdown: SIGTERM=SHUTDOWN_REQUEST (drain-not-die),
       SIGUSR2=WAKEUP_STOP (one final cycle then exit) wired in
       thread_child_signal_interrupt; SIGQUIT=PROC_DIE now honored in
       ProcessPgArchInterrupts (ProcDiePending -> proc_exit(1), mirroring the
       WAL summarizer) -- without that the archiver fiber stayed parked on
       immediate stop and the postmaster had to SIGKILL it.  Validated:
       launches as a fiber, archives WAL (archived_count=4/files=4), fast +
       immediate stop clean (0 cores, 0 SIGKILL escalations).  Permanent smoke
       gate: scripts/xtc_smoke.sh step 10.
     - Tier D: B_BG_WRITER, B_CHECKPOINTER -- DONE (in-process thread carriers
       via the early-start process->thread hand-off).  Verified 2026-07-09 on a
       primary: 0 forked child processes; the log shows "starting checkpointer
       thread carrier" and "starting background writer thread carrier"; CHECKPOINT
       works; fast AND immediate stop clean (0 cores, 0 SIGKILL).  They run as
       dedicated in-process THREAD CARRIERS (not fibers), which is the right
       model for these singletons -- they do not need fiber multiplexing, only
       to be in-process so their cross-thread wakes reach fibers.  (The
       checkpointer handoff pgstat-is_shutdown bug was already fixed.)
     - Tier E: B_STARTUP -- DONE (runs as an in-process "startup thread
       carrier"; verified in the same family dump; the gist_xlog_cleanup
       teardown SIGSEGV that had blocked it is fixed).
     - Tier F (OPTIONAL / DEFERRED, not a gap): B_IO_WORKER as fibers.  The
       io_method=worker->xtc routing (see below) is the intended threaded IO
       model, not a stopgap: every buffered read/write goes through the issuing
       fiber's own cooperative issuer-async AIO (xtc_aio_preadv/pwritev), so no
       io worker pool runs under multithreaded=on at all.  The xtc read AND
       write paths are validated under heavy concurrency + parallel workers +
       checkpoints (0 cores).  io-workers-as-fibers is only needed if the
       separate worker-pool parallelism of io_method=worker is ever wanted; if
       so it needs the AIO shutdown protocol (PM_WAIT_IO_WORKERS on fibers, item
       #6 step 4) and, for standbys, a PM_HOT_STANDBY hand-off
       (maybe_handoff_io_workers is gated on PM_RUN today; the primary hand-off
       works).  Not building speculatively -- see M16.

   MILESTONE (2026-07-09): under multithreaded=on a PRIMARY now runs with ZERO
   forked child processes.  Fibers (xtc_carrier_eligible): client backend,
   autovacuum launcher, background worker, WAL writer, WAL summarizer, archiver,
   WAL receiver, slotsync worker.  In-process thread carriers (via hand-off):
   checkpointer, background writer, startup.  io_method=xtc means no io workers;
   all buffered IO is per-fiber issuer-async.  Everything is in the postmaster's
   address space.  WAL/data fsync also routes through libxtc's async fiber fsync
   (xtc_aio_fsync/fdatasync) on a fiber -- pg_fsync_no_writethrough/pg_fdatasync
   in fd.c park the fiber instead of blocking the carrier loop; durability
   verified via SIGKILL+crash-recovery (commit 90815362fd5).  WAL page WRITES
   still use direct pg_pwrite (a fiber briefly blocks its loop -- a minor future
   perf item, not a correctness gap).
   Any future on-demand-with-start-timeout family reuses the autovac orphan
   reaper (ReapOrphanedThreadedWorker); the early-start families share the
   hand-off path.
   TEST-ENV CAVEAT: 001_threaded_runtime hangs at its background_psql section
   in THIS meson-on-btrfs environment -- an IPC::Run harness interaction, NOT a
   runtime bug: it fails IDENTICALLY on libxtc v1.2.1 and v1.3.0 (bisected),
   and the underlying behaviors (concurrent sessions, cancel, terminate) pass
   when driven directly.  Run the full 001 under `gmake check-threaded` on a
   disk-backed host, per the standing note.
6. xtc_aio for backend disk I/O.  STEPS 1-2 DONE (libxtc v1.1.0):
   plan_docs/XTC_AIO_DESIGN.md.  io_method='xtc'
   (src/backend/storage/aio/method_xtc.c, IOMETHOD_XTC/pgaio_xtc_ops) wraps
   xtc_aio_pread/pwrite (issuer-synchronous) for READV/WRITEV of ANY iovec
   length on backend fiber carriers (step 2 loops per iovec element, libxtc
   has no readv/writev); process mode and non-fiber backends fall to the
   synchronous method unchanged.  Verified on meh: multi-buffer seqscans and
   bulk-update writes are correct with 0 iovec misassembly (scripts/
   xtc_smoke.sh).  Deferred: issuer-async reap (step 3), WAL/fsync (step 4).
   B_IO_WORKER fibers (item #5) still belong to this work stream.
   STEP 3 REVIEWED against libxtc v1.3.0 (2026-07-06): DEFER, invariant holds.
   v1.3.0 adds no batch-submit convenience over the high-level AIO surface
   (xtc_aio_preadv/pwritev are one-op-per-park), so true issuer-async reap
   still means hand-rolling xtc_io_aio_submit + xtc_io_poll + per-handle tag
   plumbing on the carrier loop -- which reopens the foreign-drain (risk #1)
   and async-kill-mid-read (risk #2) hazards.  Benefit is performance-only;
   correctness is complete under Invariant A and there is no benchmark yet
   showing the sequential-per-op park is a bottleneck.  When taken up, the
   scoped first slice is batch-parallel reap within one issuer's submit(batch)
   (submit N tagged to the issuer's own task, park once, reap N -- no foreign
   drain), gated by a batch>1 test + an async-kill-mid-read fault test.  Full
   deferred reap stays behind its own design decision.  See
   plan_docs/XTC_AIO_DESIGN.md step 3.
7. xtc_orc / xtc_monitor supervision of backend fibers.  STAGE 1 DONE:
   plan_docs/XTC_ORC_SUPERVISION_DESIGN.md.  The postmaster stays the crash
   authority; xtc is an OBSERVER.  Implemented a per-loop supervisor fiber
   (xtc_carrier_supervisor_proc in pg_xtc_carrier.c) that xtc_monitor()s each
   backend and logs its DOWN: quiet for a normal exit (reason 0; the
   postmaster still reaps via process_pm_pooled_logical_exit), LOUD for an
   abnormal exit.  Stage 1a is observability only: never reaps, never
   respawns.  ALREADY PAID OFF: it surfaced a previously-SILENT intermittent
   SIGSEGV inside libxtc's xtc_exit_self during fiber teardown (findings 2c,
   reason=-11, ~3/11 backends, contained by R1) -- likely the root cause of
   the earlier worker-fiber TAP wedge, and a libxtc item to report.
   STAGE 1b DONE (escalation wired + safe): a clean-exit ring lets the
   supervisor classify each abnormal DOWN as 'benign-teardown-fault(post-
   clean-exit)' (the 2c fault, already reaped -> ignore) vs 'GENUINE-CRASH'
   (fiber died before publishing exit -> set g_xtc_genuine_crash).  ServerLoop
   polls xtc_pg_consume_genuine_crash() at both exit-drain points and, on a
   genuine crash, logs 'terminating threaded server runtime after backend
   fiber crash' + ExitPostmaster(1) -- the same policy a crashed thread
   carrier triggers.  VERIFIED SAFE on the 24-loop pool (meh): in normal ops
   GENUINE-CRASH=0 and escalation=0 while benign faults are correctly ignored,
   so the known 2c fault cannot false-crash the cluster.  A PG_XTC_INJECT_CRASH
   debug hook exists to exercise the genuine path, but reliably observing the
   injected DOWN still races the spawn->register->monitor window; production
   is covered because real backends run ~100ms of init before any crash AND
   libxtc's xtc_monitor of an already-dead pid delivers an immediate DOWN.
   FOLLOW-UP DONE (libxtc v1.3.0): the spawn/register race is closed by
   construction -- the supervisor now uses the atomic xtc_proc_spawn_monitor()
   so the monitor is in place before the child runs, and the injection test is
   deterministic (PG_XTC_INJECT_CRASH=2 -> DOWN kind=SIGNAL signal=11 ->
   escalation -> postmaster DOWN, verified on the 8-loop pool).  DOWN
   classification also moved to the self-describing xtc_down_decode_ex()
   (kind/signal/exit_code), retiring the old clean-exit ring and the
   reason-range heuristic.  The NOPROC monitor-race case can no longer occur.
8. cassert build: DONE.  All FOUR base-tree cassert aborts fixed (all
   reproduce with xtc disabled -- session-runtime-refactor bugs, not xtc):
   (1) GUCMemoryContext read side effect (peek accessor); (2) early
   aset-freelist emptiness false invariant (drop asserts); (3) numExternalFDs
   underflow at EXIT (storage closed-state reset zeroed the WaitEventSet-owned
   FD counter before the ipc bucket released those fds -- preserve across
   PgBackendResetStorageClosedState, commit 894fee47e99); (4) numExternalFDs
   underflow in a FORKED CHILD (fork_process reset zeroed the inherited count
   before ClosePostmasterPorts released the postmaster's death-pipe +
   pm_wait_set fds -- preserve across PgBackendResetEarlyFallbackAfterFork,
   commit 3bcee4eff42).  #3 and #4 share a theme: the refactor turned fd.c's
   numExternalFDs process-global into a per-state cell, and two reset points
   zero it out from under FDs other subsystems release later.  A full cassert
   cluster (process mode) now does initdb + start + query + fast stop with ZERO
   TRAP/PANIC; non-cassert xtc smoke 11/11.  Full report:
   /tmp/pg-bootstrap-cassert-bugs.md; ledger detail in M16_XTC_CARRIER_FINDINGS.md.
   ALSO FIXED (same session): a fifth cassert abort blocking THREADED-mode
   startup -- Assert(ThreadedGUCMutexDepth > 0) in ThreadedGUCUnlock, because it
   re-read the `multithreaded` flag that setting `multithreaded=on` flips
   between the paired lock/unlock; now driven off the self-describing per-carrier
   depth counter (commit 639323786c6).  This same accounting bug was the actual
   root cause of the #5 concurrent-GUC-SET/RESET wedge (below).

StartupProcess teardown SIGSEGV: FIXED (2026-07-06).  Under multithreaded=on,
after a non-clean shutdown recovery ran and the StartupProcess crashed in
proc_exit -> PgBackendResetXLogClosedState -> MemoryContextDelete
(gist_xlog_op_context).  Root cause: gist_xlog_cleanup() (RM_GIST rmgr cleanup)
deleted opCtx but left the gist_xlog_op_context cell dangling; the per-backend
teardown then deleted it again -> double-free.  Fixed by NULLing opCtx after
delete, matching spg_/_bt cleanup (commit 1b160ccf39a).  Core-dump backtrace
confirmed the dangling "GiST temporary context" header; recovery now completes
cleanly (ready to accept connections, 0 cores) with and without GiST WAL.

Remaining pre-existing NON-xtc note (do not chase as an xtc problem): if a
threaded teardown ever wedges such that fast-stop cannot complete, move the
cluster aside and re-initdb.


## 3. Debugging the concurrent lost-wakeup (the live blocker)

The symptom: 2+ fibers parked in xtc_proc_wait_fd, a wake event arrives,
exactly one fiber resumes.  Approach:

- Instrument the wake path: log per-fiber xtc_proc_wait_fd return +
  revents + the resuming fiber's pid=(loop,local,gen).  Minimal repro is
  2 concurrent socket-hold backends -- do not debug at N=4.
- Check whether the single loop's readiness dispatch wakes ALL fibers
  whose fd is ready, or only the first it finds.  A single-loop carrier
  multiplexes many fibers; each backend fiber parks on its OWN client
  fd, so a disconnect should wake exactly that fiber -- if two disconnect
  "at once" (same loop turn), the loop must dispatch both.
- Consider the mailbox path: xtc_proc_wait_fd waits on fd-readiness OR
  the proc mailbox (XTC_WAIT_MAILBOX) OR timeout.  A SetLatch is an
  xtc_send (mailbox); confirm a mailbox wake unparks the right fiber.
- Fastest likely fix: a real carrier pool (n_loops > 1) so each backend
  is on its own loop -- the single-loop edge disappears.  Try that
  before deep-diving the single-loop dispatch.

Do NOT mask it (a poll/timeout hack that "usually" wakes).  A lost
wakeup in a scheduler is a correctness bug; find the missed edge.


## 4. libxtc DOs / DON'Ts (hard-won)

DO:
- Link the built static lib OR vendor the amalgamation.  For a hand-off,
  the amalgamation (single-file xtc.c + xtc.h, built with libxtc's
  dist/mkamalgamation.py) is cleaner: no separate libxtc build, no
  absolute XTC_ROOT, no liburing/openssl link deps (it uses the ucontext
  substrate).  The .a link is what the spike used; keep XTC_ROOT
  overridable (it now is: `XTC_ROOT ?=`).
- Use the public API from XTC_ROOT/src/inc: xtc_proc.h (procs, mailbox,
  xtc_send/xtc_recv, xtc_proc_wait_fd, xtc_exit_self, xtc_proc_spawn),
  xtc_app.h (app/loop bringup), xtc_exec.h (multi-loop executor),
  xtc_loop.h (wakers, xtc_yield).  The pg_xtc_glue.* Latch mapping
  (SetLatch->xtc_send, WaitLatchOrSocket->xtc_proc_wait_fd,
  ResetLatch->drain mailbox) is the reference; it came from libxtc's
  examples/09_pgmock/pg_latch.*.
- A blocking wait inside a fiber MUST park via the fiber (xtc_yield /
  xtc_proc_wait_fd), never block the carrier thread in a raw syscall --
  that stalls every other fiber on that loop.
- Stop the server ONLY with `pg_ctl -m fast stop` (section 2).
- Clean up core dumps immediately (find -name 'core.*' -delete); a
  wedged carrier + PG's core-on-crash generates many.  NEVER commit
  them.
- ASCII-only in code + docs + commit messages (both projects' rule).
- Commit to this branch frequently; agent sessions get cut off, so
  small committed steps survive.

DON'T:
- DON'T __thread-ize stock PG's globals to force fork-elimination -- that
  is the wall the base tree already climbed; build ON it.
- DON'T block the carrier thread inside a fiber (see above).
- DON'T run PG auxiliary/startup processes on xtc yet (only B_BACKEND is
  ready); leave them on the base tree's carriers/fork until section 2
  items 5-7.
- DON'T write `#if defined(__has_feature) && __has_feature(x)` in ONE
  #if -- Ubuntu/CI gcc evaluates the second term even when the first is
  false and errors ("missing binary operator before token '('").  Use a
  NESTED #if:
      #if defined(__SANITIZE_ADDRESS__)
      #  define UNDER_ASAN 1
      #elif defined(__has_feature)
      #  if __has_feature(address_sanitizer)
      #    define UNDER_ASAN 1
      #  endif
      #endif
  (This bit the libxtc project three times.)
- DON'T assume single-backend behavior scales to N; the concurrent
  lost-wakeup proves the loop's multi-fiber dispatch needs its own
  verification.


## 5. Techniques from libxtc worth reusing

- Deterministic Simulation Testing (DST): libxtc has a seeded, single-
  thread simulator (docs/M_DST.md) that replays scheduling bit-
  identically and injects faults/partitions/crashes.  The concurrent
  lost-wakeup is EXACTLY the class of bug DST catches deterministically.
  Consider a small DST harness that drives N fibers parking + waking on
  the carrier loop, to reproduce + fix the wedge reproducibly rather
  than racing psql.  (libxtc found ~7 real concurrency bugs this way,
  including several lost-wakeups.)
- xtc's work-stealing executor scales ~10x on 8 cores in isolation; the
  carrier POOL should lean on it (item 4).
- Fiber memory floor is ~one committed page per parked fiber -- cheap
  enough for many idle backends (the connection-scalability goal).


## 6. Where things live

- The xtc carrier seam: src/backend/postmaster/pg_xtc_carrier.c/.h,
  the B_BACKEND route in src/backend/postmaster/launch_backend.c, the
  socket-wait intercept in src/backend/storage/ipc/waiteventset.c, the
  Latch glue src/backend/port/pg_xtc_glue.c/.h, the link wiring in
  src/Makefile.global.in (gated on USE_XTC_CARRIER).
- Build: `make USE_XTC_CARRIER=1 XTC_ROOT=/path/to/libxtc ...` after
  building XTC_ROOT/build_unix/libxtc.a.
- The running record: M16_XTC_CARRIER_FINDINGS.md (milestone, recipe,
  proof, walls).  Keep it CURRENT -- it is the memory across agent
  hand-offs.
- libxtc itself: XTC_ROOT (AGENTS.md, PLAN.md, docs/M16_PG_ADAPTER.md,
  docs/M_DST.md, examples/09_pgmock/ = the in-tree mock backend that
  proves the seam with zero PG source).
