# Retest on libxtc v1.41.0 as requested: the hang PERSISTS (4/6), same surface-#7 signature

Date: 2026-09-06
libxtc: v1.41.0 (rev 9c5c5ff).  Fix 53e6ea1 CONFIRMED present in my build
(src/evt/task.c: atomic_store_explicit(&w->task->wake_pending, 1, ...) in the same-loop
branch), and confirmed an ancestor of v1.41.0 but not of v1.40.7 -- so this is a genuine
retest of the fix, not a stale build.
Re: your 2026-09-06 reply (surface #7 = same-loop waker dropping the wake on CAS failure).

--------------------------------------------------------------------------------
## Result: still hanging

Your requested acceptance run (scale=50, c=32, fsync=on, NVMe, 32 vCPU, fresh server per
run, 20 runs, 100s timeout on a 20s bench):

    run 1: HANG (rc=124)
    run 2: HANG (rc=124)
    run 3: HANG (rc=124)
    run 4: HANG (rc=124)
    run 5: PASS  tps = 1469.76
    run 6: PASS  tps = 3135.03
    run 7: harness wedged (see note) -- aborted the sweep there

So 4 hangs in the first 6 completed runs.  Not the "20 clean runs" the fix predicted.

## The signature is IDENTICAL to surface #7 (not a new presentation)

At the first hang, the artifacts you asked for:

    park kinds:   34 park=-     35 park=fd     1 park=timer
    proc states:  all parked (no `scheduled`, no `running` besides the gdb-attached thread)
    bt x3 (1s apart, byte-identical):
        xtc_aio=0  fdatasync=0  XLogWrite=0  issue_xlog_fsync=0
        iou-wrk=19  xtc_io_poll=31  LWLockAcquire=1

And on a second, independent hang caught later (run 7, which hung during the SINGLE-connection
`pgbench -i` load phase -- notable, see below), with pg_stat_activity available:

    pid  7  client backend    active  IO/WalSync      COPY pgbench_accounts FROM stdin WITH (freeze
    pid  9  autovacuum worker active  LWLock/WALWrite VACUUM ANALYZE pg_catalog.pg_depend
    pid 11  autovacuum worker active  LWLock/WALWrite VACUUM ANALYZE pg_catalog.pg_collation
    pid 13  autovacuum worker active  LWLock/WALWrite ANALYZE information_schema.sql_features
    walwriter/checkpointer/bgwriter: idle
    park kinds:  34 park=-   5 park=fd   1 park=timer      proc states: 39 parked, 1 running
    bt: xtc_aio=0 fdatasync=0 issue_xlog_fsync=0 iou-wrk=2 io_poll=31

Same shape as before: exactly ONE backend in IO/WalSync (it holds WALWriteLock inside
issue_xlog_fsync -> xtc_aio_fdatasync), that fiber is on NO OS thread (parked, not spinning),
its io_uring workers are present (submitted+serviced), and everything else queues on
LWLock/WALWrite behind it.

## What I could NOT capture, and why

You asked for the stuck fiber's `task->state` (PARKED vs RUNNING) and whether `wake_pending`
is set -- the discriminator between "regression of #7" and a 9th surface.  I could not get it:
libxtc's task/proc tables are not exported symbols (the `xtc_*`-only symbol gate strips
`__xtc_*`), so from gdb on a release build I can reach `g_xtc_exec` and `g_xtc_n_loops` but
cannot walk tasks to read `state`/`wake_pending`.

If you want that datapoint, either:
  (a) send the gdb helper you offered earlier (a tools/gdb/xtc-gdb.py that walks loops ->
      tasks and prints state/park_fd/wake_pending), or
  (b) tell me a build flag that exports the task table (or a debug/DIAGNOSTIC build I should
      use), and I will capture it on this repro within one cycle.
I have the repro running reliably in ~2 minutes per attempt, so a turnaround on that is cheap.

## One correction to my previous report (please disregard that hypothesis)

I previously offered "three amplifiers" (lock held across the fsync + ~33 socket-parked fibers
+ steal churn) and suggested a harness delta built on them.  Two new datapoints refine that:

 - Run 7 hung during a SINGLE-CONNECTION `pgbench -i` (one client doing COPY/index/vacuum),
   with only autovac workers as other activity.  So ~33 concurrent socket-parked sessions are
   NOT required.
 - But a dedicated 10x single-connection `pgbench -i` sweep on a FRESH server each time was
   9/9 PASS -- so init-alone is not sufficient either.
Reading: what matters is that the loops are in an active steal/migration state (left that way
by preceding concurrent load, or by the autovac/aux fibers), not the raw session count.  I no
longer claim the three-amplifier framing; the reliable trigger remains "run the 32-client
bench", and the single-connection case can hang once the loops are churning.

## Harness note (mine, not yours)
Run 7's hang was in `pgbench -i`, which I had not wrapped in a timeout, so my sweep blocked
there instead of continuing to run 8.  Fixed locally; it does not affect the 4/6 result above.

## Environment
PG "xtc" HEAD (includes our hot-cell thread-safety fix and the ProcSleep bounded-wait guard),
libxtc v1.41.0, c6id.8xlarge (32 vCPU), PGDATA on local NVMe/xfs, shared_buffers 40% RAM,
fsync=on, synchronous_commit=on, multithreaded=on, pooled_protocol_carriers=0.
