# Surface #7 on libxtc v1.40.7: a real fiber-path hang -- NOT a contention artifact (falsifying test)

Date 2026-09-04 (bene, c6id.8xlarge NVMe, libxtc v1.40.7, PG HEAD 2c9e45eaa5).

## The hang state (fully characterized)
At a caught hang (commits_delta=3 over 4s):
 - pg_stat_activity (client backends): 58 on Lock/tuple, 6 on Lock/transactionid, 1 running.
 - pg_locks NOT granted: tuple/ExclusiveLock x57, transactionid/ShareLock x5.
 - xtc_dump: ALL 100 procs "parked" (67 park=fd, 32 park=-, 1 park=timer).  ZERO spinning,
   ZERO runnable-but-unscheduled.  The v1.40.7 io->fds spin is gone (0 io_del_fd/__find_fd
   frames).
 - NO LWLock contended at all (no LWLockAcquire frame in any bt) -- WALWriteLock is NOT
   involved this time, unlike surfaces #1-#6.
 - All 32 loops healthy/alive with tasks_run + steals climbing.
 - Aux workers (walwriter/checkpointer/bgwriter) idle-normal; the epoll_wait frames seen
   earlier are just those aux pthreads in their normal latch waits.

## FALSIFYING TEST: it is NOT a pgbench row-lock convoy / benchmark artifact
The initial read was "scale-50 with 64 clients = pathological tuple contention, so it is
just slow."  Tested directly (20s runs, 100s timeout, fresh server each):
  fiber  scale=50  clients=64 -> HANG (rc=124)
  fiber  scale=300 clients=64 -> HANG (rc=124)   <-- 6x more rows, 10x fewer conflicts
  fiber  scale=50  clients=16 -> HANG (rc=124)   <-- 4x fewer clients
  FORK   scale=50  clients=64 -> rc=0, tps = 42,282   <-- SAME pathological workload, fine
So reducing contention (both by scale and by client count) does NOT fix it, and fork sails
through the identical workload.  This is a REAL fiber-path bug, not a workload artifact.

## Correct reading of the lock convoy
The 57 tuple waiters are a SYMPTOM, not the cause: one session fiber holds a row lock and is
parked-and-never-resumed, so every other session queues behind it on that row.  The lock
convoy is the consequence of the stranded holder.  (Same shape as surfaces #1-#6 -- a
stranded holder -- but the held object is a heavyweight row lock, not WALWriteLock, and there
is no spin.)

## What is different from #1-#6 (and what to chase next)
 - No spin (v1.40.7 fixed that), no LWLock, all procs cleanly "parked".
 - So the stranded holder is parked on one of: park=fd (its client socket / sem eventfd) or
   the single park=timer.  67 park=fd for ~64 sessions means essentially every session is
   parked on an fd -- including the holder.  The holder is therefore waiting for an fd
   readiness/wake that never arrives, OR it completed its work and its resume was lost.
 - NEXT STEP: identify WHICH parked proc is the row-lock holder and what fd it waits on.
   Approach: correlate PGPROC (the granted-tuple-lock holder from pg_locks) -> its
   sem_fiber_loop/local/gen handle -> the matching <loop.local.gen> line in xtc_dump, and
   read that proc's park_fd.  Then determine whether that fd is its client socket (waiting on
   a client that is itself blocked = a PG-level protocol/flush deadlock) or its sem_wake_fd
   (a lost cross-fiber wake = libxtc/PG wake path).
 - Also unexplained and worth a separate look: even SUCCESSFUL fiber runs are only ~2.4-3.0k
   tps vs stackless pool 37.9k / fork 42-46k.  That gap is not explained by the hang.

## Status
Option A default flip stays staged/dormant (hang rate ~60% at c=64; and now shown to hang
even at c=16).  Do NOT file a libxtc report yet: ownership is undetermined -- the holder may
be waiting on its CLIENT socket (a PG-side issue) rather than a lost runtime wake.  The
PGPROC->fiber->park_fd correlation above decides it.
