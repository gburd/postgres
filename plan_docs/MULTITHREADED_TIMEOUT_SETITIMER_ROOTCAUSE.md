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
