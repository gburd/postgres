# RCA: HammerDB monitor-VU hang on the pooled threaded server (2026-08-27, mala)

## Root cause (found)
A pooled carrier that found no runnable/queued work slept in
backend_pooled_protocol_wait_for_work (xtc_pg_pooled_queue_wait), which wakes ONLY on
newly QUEUED work.  It did NOT watch already-parked sessions' sockets.  When a parked
session's socket became readable (its next query arrived) and every carrier was idle,
the readiness was observed by nobody -> the session hung.  High-frequency workers keep a
carrier in WaitParkedReads (polling the fds), so they rarely hang; a low-frequency idle
session -- HammerDB's monitor VU (periodic `select sum(xact_commit+xact_rollback) from
pg_stat_database`) -- was the reliable victim.  Confirmed by gdb: carriers idle in
xtc_pg_pooled_queue_wait / xtc_io_poll while the monitor's query never got a carrier (a
district query in the same instant returned <1ms).

## Fix (landed, PG side)
PgRuntimeProtocolSchedulerWaitParkedReads now reports (bool *had_parked) whether it
leased >=1 parked session to poll; the carrier loops back to re-poll (bounded by the
existing 1000ms WaitParkedReads timeout) instead of sleeping on the queue-only
wait_for_work.  At least one carrier keeps the parked fds continuously watched.  The
isolated hang (a single pg_stat_database query under 8 write workers) is GONE (rc=0).

## Residual (filed to libxtc)
Under sustained load the hang still occurs intermittently at the LIBXTC level: gdb shows
carriers idle in xtc_io_poll (io_uring wait), below the PG queue -- a cross-loop wake to
an idle io_uring loop is missed (our own launch_backend.c:244 comment predicted this).
Reported: plan_docs/phase16_audits/LIBXTC_IOLOOP_CROSS_WAKE_MISS.md.

## Comparable threaded NOPM (measured hang-immune, by counting committed new-orders
## in the server log over a 180s steady window -- the exact NOPM metric, no client query)
c6id.8xlarge, NVMe xfs, TPROC-C, durability ON, VU=64, libxtc v1.38 + this fix:
  process (fork):            NOPM = 97,123   (291,370 neword / 180s)   1.00x
  threaded (pooled):         NOPM = 43,143   (129,431 neword / 180s)   0.44x
The PG-side lost-wakeup fix raised threaded from the earlier ~0.24x (when the monitor
happened to capture it) to ~0.44x by cutting stranded-session stalls; the residual gap
is the libxtc io-loop wake-miss + the fundamental write-path scheduler-feeding gap
(carriers under-utilized under write load; separate, documented).
