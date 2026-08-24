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
