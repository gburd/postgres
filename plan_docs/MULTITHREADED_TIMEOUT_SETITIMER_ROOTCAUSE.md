# ROOT CAUSE (2026-09-01): the fiber-path intermittent write stall is PG's process-wide setitimer/SIGALRM timeout under threading -- NOT a libxtc bug

## Definitive diagnosis
The Option-A fiber path (pooled_protocol_carriers=0) intermittently stalls under
pgbench -c 64 write load.  Root-caused on EC2 (libxtc v1.40.6, all 5 prior cross-loop
libxtc fixes in, PG hot-cell fix in):

At a caught stall (commits delta=2/4s): WALWriteLock held exclusive (state 0x80042000);
the WAL writer waits on it; ~all fiber backends are in PROC_WAIT_STATUS_WAITING (waitStatus=1)
on HEAVYWEIGHT lock-manager locks, MANY on the SAME lock addresses (e.g. 0x...ab518 x3,
0x...ffe9c40 x3) -- i.e. a pgbench row-lock (pgbench_branches/tellers) contention CYCLE.
31 loops idle in xtc_io_poll; a few procs armed=1/waitStatus=0 hold the locks others wait
on.  No fiber in xtc_aio/fdatasync/exec_simple on any stack -- so it is NOT the aio
completion (libxtc verified sound) and NOT a scheduler strand.

The lock cycle is never broken because **PG's deadlock detector never fires for the waiting
fibers.**  ProcSleep arms DEADLOCK_TIMEOUT (+ LOCK_TIMEOUT) via enable_timeout_after and then
WaitLatch(MyLatch, WL_LATCH_SET, timeout=0, ...) -- i.e. INFINITE wait, relying on the timer
to set got_deadlock_timeout + SetLatch.  But timeout.c arms the timer with
**setitimer(ITIMER_REAL) + SIGALRM -- a PROCESS-WIDE timer + a signal delivered to an
arbitrary thread.**  Under the multithreaded fiber runtime all 64 fiber backends share ONE
process interval timer: each arm CLOBBERS the previous (last-writer-wins on the single
ITIMER_REAL), and SIGALRM lands on whatever thread, whose handler services only ITS fiber's
timeouts.  So a fiber blocked in ProcSleep on a lock cycle typically never gets its
DEADLOCK_TIMEOUT delivered -> CheckDeadLock never runs -> the cycle is never broken ->
those fibers wait forever -> commits freeze.

Intermittent because it needs a real lock cycle to form (probabilistic at 64-client row
contention).  The timeout STATE is already per-fiber (signal_pending/signal_due_at via
PG_RUNTIME_FAST_BUCKET_ACCESSOR CurrentPgBackendTimeoutRuntimeState), but the ARM
(schedule_alarm -> setitimer ITIMER_REAL) and the SIGALRM delivery are process-wide and
unseamed for threading.  This also breaks statement_timeout, lock_timeout, idle_*_timeout
for fiber backends -- deadlock is just the one that wedges the write path.

## This is PG-side, not libxtc
libxtc's timer/park primitives are sound (team verified v1.40.6; aio path verified).  The
gap is entirely in PG's timeout.c using a process-global setitimer under a runtime that now
has many fibers per process.  No libxtc report is warranted.

## Fix design (per-fiber timeout, no process-wide setitimer under multithreaded)
Under `multithreaded`, a fiber backend must not arm ITIMER_REAL.  Options:
 A. (preferred, least-invasive) In schedule_alarm, when the caller is a backend fiber
    (xtc_in_backend_fiber / CurrentPgCarrier kind THREAD), DO NOT setitimer.  Instead the
    fiber's own bounded park delivers the timeout: ProcSleep (and every WaitLatch/WaitEventSet
    the fiber does) already can carry a finite timeout, and libxtc's xtc_pg_wait_fd supports
    a deadline (xtc_proc_sleep-backed).  Make the fiber's blocking wait use
    min(nearest_timeout - now, ...) as its park timeout, and on wake re-run handle_sig_alarm-
    equivalent (fire due timeouts, set got_deadlock_timeout) locally.  i.e. move from
    "process SIGALRM sets the flag" to "the fiber's own timed park sets the flag on wake."
 B. A per-fiber libxtc timer (xtc timer callback) that sets the fiber's signal_pending +
    SetLatch(MyLatch) -- a per-fiber replacement for the one ITIMER_REAL.
Option A is cleaner: it reuses the existing timeout bookkeeping (active_timeouts,
signal_due_at) and the fiber's existing park, just changes schedule_alarm to skip setitimer
on a fiber and makes the fiber's wait deadline-bounded so it wakes to service due timeouts.
Both keep process mode BYTE-FOR-BYTE (setitimer path unchanged when not a fiber).

## Guardrails
Process mode byte-for-byte (gate on xtc_in_backend_fiber / multithreaded).  Two-review
(deadlock detection is correctness-critical).  Validate: (1) the fiber-path pgbench -c 64
stall is GONE across many runs; (2) a deliberate deadlock (two sessions cross-locking) is
detected + broken within deadlock_timeout on the fiber path; (3) statement_timeout /
lock_timeout fire correctly on a fiber; (4) 245/245 process regress unchanged; (5) the
isolation/deadlock TAP + timeouts tests green threaded.

## REFINEMENT (2026-09-01, deeper trace): it is the LOGICAL-timeout delivery for fiber lock-waits, NOT setitimer/PostgresMain

Corrected the earlier "setitimer/PostgresMain unconditional" hypothesis by reading the code:
- PostgresMain (postgres.c:7255) is ALREADY correctly gated:
    if (threaded_backend) InitializeLogicalTimeouts(); else InitializeTimeouts();
  So a fiber client backend uses LOGICAL timeouts (timeout_signal_delivery=false), NOT
  setitimer/SIGALRM.  My "process-wide setitimer clobber" mechanism is therefore NOT what a
  fiber backend hits.  (backend_startup.c also gates InitializeTimeouts to
  BACKEND_STARTUP_PROCESS only.)
- The logical-timeout machinery is fully built and per-fiber: schedule_alarm's
  !timeout_signal_delivery branch skips setitimer and records signal_due_at/alarm_enabled
  (per-fiber bucket); WaitEventSetWait clamps block_timeout to get_logical_timeout_delay_ms()
  and calls process_due_logical_timeouts() on a timeout return; that fires due timeouts +
  sets got_deadlock_timeout.  ProcSleep waits via WaitLatch(MyLatch, 0) -> WaitEventSetWait,
  which is exactly the clamped path.  alarm_enabled / signal_pending / signal_due_at /
  num_active_timeouts are all per-fiber (PG_RUNTIME_FAST_BUCKET_ACCESSOR
  CurrentPgBackendTimeoutRuntimeState).

So on INSPECTION the logical-timeout path is correct and should fire the DEADLOCK_TIMEOUT on
the waiting fiber.  Yet empirically, under 64-client row contention, a fiber lock-cycle is
not broken (commits freeze; the stall bt shows fibers in PROC_WAIT_STATUS_WAITING on
heavyweight locks with no deadlock resolution).  The bug is therefore a SUBTLE RUNTIME EDGE
in the logical-timeout delivery for fiber lock-waits, not a missing gate.  Candidates to
INSTRUMENT (the definitive next step):
  1. Does get_logical_timeout_delay_ms() actually return the DEADLOCK deadline while a fiber
     is parked in ProcSleep's WaitLatch, or -1 (e.g. alarm_enabled cleared by a prior
     process_due_logical_timeouts disable_alarm() and not re-enabled, so the clamp is
     skipped and the park is infinite)?
  2. When the clamp IS applied, does the fiber's xtc_pg_wait_fd actually wake at the
     deadline (a v1.40.6 timer-park corner) and does the WaitEventSetWait loop then call
     process_due_logical_timeouts()?
  3. Does fire_due_timeouts() run CheckDeadLock's handler (set got_deadlock_timeout) for the
     RIGHT fiber, and does ProcSleep's loop re-check got_deadlock_timeout after the clamped
     wake?
  4. Interaction: the LWLock fiber-park (ProcSemaphoreWaitFiber -> xtc_pg_wait_fd(sem_wake_fd,
     -1) INFINITE) is a SEPARATE, un-clamped park used for LWLock deep-waits.  A heavyweight
     LOCK wait uses WaitLatch (clamped); but if any part of the WAL-commit lock chain waits
     via the infinite sem park, THAT wait ignores the deadlock deadline.  Verify which park
     each contended lock in the wedge uses.

Hypothesis (4) is the most likely: ProcSemaphoreWaitFiber parks INFINITE (timeout=-1) and is
used for LWLock/buffer-content/group-update deep-waits -- NONE of which consult the logical
timeout.  If the cycle involves an LWLock deep-wait (WALWriteLock is an LWLock!), the waiter
parks forever with no deadlock check -- but LWLocks are not deadlock-checked in PG anyway
(they are acquired in a fixed order).  The heavyweight row locks (transactionid/tuple) DO
use WaitLatch + deadlock detection.  So instrument (1)/(3) on the heavyweight path first.

NEXT: instrument get_logical_timeout_delay_ms + process_due_logical_timeouts (elog DEBUG) on
a fiber under the wedge repro; catch whether the clamp fires and whether CheckDeadLock runs.
Do NOT ship a fix into the deadlock-detection path without that runtime evidence.  This is
PG-side (no libxtc report); the fix will be in the logical-timeout delivery edge, once the
instrument names it.

## PINNED (2026-09-01, instrumented): hang = DEADLOCK_TIMEOUT not armed on the fiber (num_active=0, logical_delay=-1) -- a per-fiber timeout-bucket consistency issue

Added elog trace to ProcSleep (PG_XTC_TIMEOUT_TRACE) logging logical_delay_ms +
num_active + deadlock_due + got_dl at each fiber lock-wait park, and whether CheckDeadLock
fires.  Two run classes, definitive:

PROGRESSING run (commits ~600/s): every ProcSleep park shows
  logical_delay_ms=1000 num_active=1 -> the DEADLOCK_TIMEOUT IS armed, the logical clamp
  fires, CheckDeadLock ran 394x, cycles broken, commits flow.  The machinery WORKS.

HANG run (commits delta=2, frozen): every ProcSleep park shows
  logical_delay_ms=-1 num_active=0 got_dl=0, and during the hang CheckDeadLock fires
  delta=0, park delta=0 (everything frozen).  So on the hung fiber the DEADLOCK_TIMEOUT was
  NOT armed (num_active=0) -> get_logical_timeout_delay_ms returns -1 -> the fiber's
  WaitLatch park is INFINITE with no deadlock deadline -> a lock cycle is never broken ->
  permanent hang.

Why num_active=0 after enable_timeout_after(DEADLOCK_TIMEOUT)?  The timeout state
(all_timeouts[], num_active_timeouts, all_timeouts_initialized) is a PER-FIBER bucket
resolved through the HOT CURRENT-WORK accessor:
  CurrentPgBackendTimeoutRuntimeState -> PG_RUNTIME_FAST_BUCKET_ACCESSOR(...,
  PgCurrentTimeoutState) -> resolved via CurrentPgBackend (the hot current-work ref cells).
enable_timeout has Assert(all_timeouts[id].timeout_handler != NULL) -- COMPILED OUT in the
cassert=false production build.  So if, at the enable_timeout_after moment, CurrentPgBackend
(hence the timeout bucket) is inconsistent/stale for a migrating fiber -- the same hot
current-work cell consistency class we already touched -- the arm lands on the wrong (or an
uninitialized/unregistered) bucket, num_active stays 0, and the subsequent park reads a
bucket with no active deadlock timeout -> infinite park.  Intermittent because it depends on
the fiber's migration/current-work timing at that instant.

## This ties the timeout hang to the current-work-bucket consistency on the fiber path
The earlier hot-cell fix (mode-state thread-local + guarded process-cell clears) addressed
TSan-visible races but did NOT guarantee CurrentPgBackend is CONSISTENT across an
enable_timeout_after -> WaitLatch-park sequence on a migrating fiber.  The timeout bucket is
one concrete victim; there may be others (any PER-FIBER bucket read after a current-work
switch).  Still PG-side; NOT libxtc (libxtc timer/park primitives verified sound).

## FIX DIRECTION (do NOT rush into the current-work/deadlock core)
Candidates, needs one more targeted check (does CurrentPgBackend actually differ between the
enable_timeout_after and the park, or is the bucket simply never registered for this fiber?):
 A. If it is a stale-CurrentPgBackend read: ensure the current-work roots are pinned/
    consistent for the whole ProcSleep enable+park window on a fiber (the roots must not be
    repointed by a concurrent carrier between arming and parking).  This is the deeper
    hot-cell consistency fix.
 B. If it is a missing per-fiber registration: guarantee RegisterTimeout(DEADLOCK_TIMEOUT..)
    ran for THIS fiber's bucket before ProcSleep, and make enable_timeout hard-fail (elog,
    not a compiled-out assert) when a timeout is enabled on an unregistered id under
    multithreaded -- so this can never silently no-op into an infinite park.
 C. Belt-and-suspenders: ProcSleep on a fiber must NEVER park infinitely for a lock wait --
    always bound the WaitLatch by min(deadlock_timeout, ...) directly (not solely via the
    logical clamp), so a bucket glitch degrades to a slow deadlock check, never a hang.
Option C is the safest immediate guard (bounds the blast radius to "slow" not "hung");
A/B are the true fix.  Two-review (deadlock-detection correctness).  The trace instrument
(PG_XTC_TIMEOUT_TRACE) stays available (env-gated, zero-cost off) for the next session to
run check A vs B.

## FINAL (2026-09-01, instrumented WaitLatch return): the root is a libxtc xtc_proc_wait_fd TIMEOUT lost-wake, NOT PG

Instrumented the exact WaitLatch return in ProcSleep's fiber wait (logging rc + WL_TIMEOUT/
WL_LATCH bits).  Definitive:
- Healthy windows: every wake is WL_LATCH_SET; WL_TIMEOUT is NEVER set even though the
  fiber passes a finite (1000ms) timeout to WaitLatch -> xtc_proc_wait_fd(timeout_ns=1e9).
- Hang windows: the WaitLatch(1000ms) returns 0 times in 4 seconds -- the fiber is stuck
  INSIDE xtc_proc_wait_fd; the timeout never fires and, with no SetLatch coming (the waker
  is itself parked behind the stuck holder), it parks forever.

So my option-C mitigation (bound the fiber lock-wait by deadlock_timeout + defensive
CheckDeadLock) is INERT: the timeout wake it relies on does not fire.  The num_active=0 /
logical_delay=-1 earlier was a real (secondary) per-fiber bucket glitch, but the PRIMARY,
sufficient cause of the hang is that xtc_proc_wait_fd's finite timeout is not delivered to a
migratable fiber under load -- a libxtc lost-timeout-wake on the fd-park path, the sibling
of the xtc_proc_sleep/xtc_recv timer-park that v1.40.6 fixed.  This is a LIBXTC bug (6th
cross-loop surface).  Filed: LIBXTC_V1406_FD_PARK_TIMEOUT_LOST.md (+ /tmp/...).

The belt-and-suspenders guard in ProcSleep is KEPT (correct code): once libxtc delivers the
fd-park timeout, it bounds any residual glitch to a slow deadlock check instead of a hang.
Ruled out this session with instrumentation: deadlock detector (works), setitimer (gated),
hot-cell race (fixed), aio completion (sound).
