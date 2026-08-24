# Pooled carrier demand-grow: fiber -S reaches/BEATS fork parity (2026-08-24, c7i.metal-48xl/192-core, chiuso)

## The fix
launch_backend.c: the pooled carrier pool grew only against the new-connection
dispatch queue (queue_length > idle_carriers), so a steady set of already-
connected sessions -- resumed in place, never re-queued -- froze the pool at ~15
carriers on a 192-core box.  Fix: grow toward RUNNABLE demand.
  - Enqueue path (maybe_start_carrier_for_work): grow to queued+busy, capped at
    min(carrier_limit, ncpus).
  - Resume path (NEW): a carrier that leases a runnable backend and sees
    runnable_count > carrier_count sets a flag + SetLatch(postmaster); ServerLoop
    calls backend_pooled_protocol_maybe_grow_for_runnable_demand() to create
    carriers (carrier creation stays on the postmaster thread -- carrier_count +
    wake-fd setup are postmaster-owned/unsynchronized).
  - Guardrail: min(limit, ncpus) ceiling holds the line against the 2026-07-23
    thread-explosion.
New accessor PgRuntimePooledProtocolRunnableCount() (reads scheduler->runnable_count).

## Result: read-only OLTP (-S), carriers=192, 15s cells
  c     fork        fiber(grow-fix)   ratio
  64    1,594k      1,473k            0.92x
  128   1,914k      1,871k            0.98x
  192   1,818k      1,850k            1.02x   <- BEATS fork
  256   1,764k      1,840k            1.04x   <- BEATS fork
Before the fix, fiber -S was flat at ~1.05M (0.55x).  Now at parity and AHEAD at
c>=192 -- the north-star result on read OLTP, and the oversubscription thesis
confirmed (fibers pull ahead where fork's per-process overhead grows).

## Still open: CPU-bound queries (generate_series compute)
  compute c=128: fork 60,842  vs  fiber ~3,900 (0.06x) -- NOT fixed.
Under CPU-bound load the pool grows to ~15-35 but ~70% of CPUs stay IDLE with
~14% futex wait: the carriers contend on the pooled scheduler lock/notify
(PgRuntimeProtocolSchedulerPopRunnable, __condvar_cancel_waiting, __lll_lock_wake,
xtc_counter_add) in the lease/resume path -- adding carriers adds contention, not
parallelism.  This is a deeper F2-layer serializer (the pooled queue lock/notify
+ scheduler runnable-queue), a SEPARATE follow-up from pool sizing.  The -S win
does not depend on fixing it.

## Discipline
This is a hot-path scheduler change -> two-review gate + world tests (process
byte-for-byte, threaded test_backend_runtime, check-threaded-pooled) BEFORE
origin/xtc.  Committed to a candidate branch; not yet pushed to xtc.

## Correctness status (2026-08-24, same metal box)
- process regress with the fix: 245/245 result files, 0 diffs -> process mode
  byte-for-byte intact.
- threaded test_backend_runtime with the fix: 17 Ok / 1 FAIL -- ONLY
  001_threaded_runtime, at subtest "PMChild reaping stress cycle N accepted
  active terminate requests" (pg_terminate_backend(pid, 5000) of ACTIVE backends
  times out: "did not terminate within 5000 milliseconds").  002-016 all pass.
- Isolation A/B (PG_XTC_NO_RESUME_GROW gate): 001 fails with the resume-path
  grow BOTH enabled AND disabled -> the resume-path grow is not the sole cause.
- Clean-control (unmodified origin/xtc, separate tree on the same box): could
  NOT obtain a valid run -- the separate-tree meson test harness bailed with
  "pg_config failed" (tmp_install/PATH artifact of the second tree), so it never
  executed 001.  The EARLIER c7i.8xlarge validation box passed test_backend_runtime
  18/0 on clean origin/xtc, which SUGGESTS the 001 terminate-stress failure here
  is environmental (heavily-loaded 192-core metal + a 5s terminate timeout that
  is timing-sensitive under load) rather than a regression -- but this is NOT
  proven without a clean control.

## Before landing (required)
1. Clean-box control: run test_backend_runtime on a FRESH quiet box for BOTH
   clean origin/xtc and the grow-fix, same harness invocation, to prove the 001
   terminate-stress result is environmental (or find+fix a real regression).
2. Two independent adversarial reviews of the diff (hot-path scheduler change).
3. check-threaded-pooled + the -S beat-fork number re-confirmed post-review.
The fix stays on branch pooled-demand-grow until all three are green.
