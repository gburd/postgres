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
   base pthreads.  INFRASTRUCTURE IN PLACE, workers still DEFERRED.
   xtc_carrier_eligible() (launch_backend.c) gates which child types run as
   fibers; the launch/exit plumbing is generalized for any type.  Only
   B_BACKEND is admitted today.  A bare pthread->fiber carrier swap for
   B_BG_WORKER passed happy-path smoke but WEDGED the threaded-runtime TAP
   (terminating an idle backend hung; the suite was a clean 129/129 before).
   Worker fibers need fiber-aware crash/terminate/restart and shutdown-
   ordering handling, which depends on item #7 Stage 1 (fiber-death
   observation).  Widen the allowlist one family at a time, each validated
   under the FULL threaded-runtime TAP (not just smoke) on a disk-backed host.
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
   FOLLOW-UP: close the spawn/register race (supervisor-owned spawn, or
   register-before-run) so the injection test is deterministic; then widen #5
   to worker fibers (now unblocked by genuine-crash observation).
8. cassert build: fix the bootstrap GUCMemoryContext == NULL assertion
   so the carrier runs under --enable-cassert (better crash diagnostics).

Known pre-existing NON-xtc bug (do not chase as an xtc problem): after a
NON-clean shutdown, recovery runs and the StartupProcess thread SIGSEGVs
in proc_exit -> PgBackendResetXLogClosedState -> MemoryContextDelete.
It corrupts the cluster.  ALWAYS stop with `pg_ctl -m fast stop`; if the
loop is wedged and fast-stop cannot complete, move the cluster aside and
re-initdb.  Reproduce on the pristine `multithreaded` branch and report
upstream.


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
