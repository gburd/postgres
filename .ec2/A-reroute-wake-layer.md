# A + supervisor-reroute: connection drops FIXED; new layer = idle->active wake throughput (2026-08-06)

## Progress this session (local commits, not pushed -- candidates)
1. 4af3e0f0084 -- fix A: postmaster answers SSLRequest 'N' at accept time
   (bounded non-blocking peek+send).  Eliminated the "SSL exchange" negotiation-
   latency drop.
2. 3432ceb8fb8 -- route the backend-fiber spawn to a LIVE supervisor + retry on
   XTC_E_AGAIN/XTC_E_INVAL instead of dropping the accepted connection.  Root
   cause of the drop was xtc_send returning XTC_E_INVAL (rc=-1) for a supervisor
   whose g_xtc_sup_pid was XTC_PID_NONE (spawn-failed at startup) or transiently
   !alive under burst; we treated it as a hard client-facing failure and dropped.

## Validated (m8idn.metal-96xl, both fixes): connection drops ELIMINATED
pgbench -S -c 384 on the tps lane: **connect_fail=0, spawn_retry_warnings=0**,
373+ backends connected (was ~6-8 dropped per burst, "could not fork ... Resource
temporarily unavailable").  So the accept/spawn drop path -- the direct cause of
the tps "NA" -- is FIXED.  fork never drops an accepted client; now neither does
the fiber path.

## NEW layer isolated: idle->active wake THROUGHPUT stall (not a drop)
With connections no longer dropping, pgbench -S -c 384 -j 64 CONNECTS all 384 but
then STALLS: pg_stat_activity shows 373 backends in Client/ClientRead (parked
waiting for the next query), 4 in LWLock/LockManager, 1 active; commits ~0/s;
pgbench itself Sl (sleeping, alive).  So the fibers are correctly PARKED on the
client-read boundary, but throughput is ~0 -- the many-idle-fibers -> active
transition is not flowing.  Hypotheses (next, instrument-first):
  - a WAKE MISS: a fiber parked in Client/ClientRead (xtc_pg_wait_fd on its
    socket) is not woken when pgbench's next query byte arrives, at high
    idle-fiber count -- the protocol-read wake path under 384 parked fibers.
  - or pgbench's 64 driver threads cannot drive 384 connections on a loaded
    384-core box (a client-side limit, not a server bug) -- rule this out by
    driving with a server-side-measured, thread-per-connection driver or fewer
    clients-per-thread.
  - the 4 stuck in LWLock/LockManager may be a related contention/wake issue on
    the lock-manager partition under the fiber wake path.

## Assessment / where this leaves the A/B
The connection-drop cause of "NA" is FIXED (real, validated).  The A/B throughput
number is now blocked by a DIFFERENT, newly-isolated layer: idle->active fiber
wake throughput at high parked-fiber counts.  That is a deep fiber protocol-read
wake-path issue (the heart of "use libxtc to the fullest" -- the wake must fire
reliably for every parked session) and needs on-box instrumentation of a parked
fiber's wake to pin (wake miss vs pgbench-driver limit vs lock-manager).

The fixes (A + reroute) are correct improvements and should land AFTER: (a) the
wake-throughput layer is understood (so we know the fixes compose with the real
unblock), and (b) the two-review gate (A touches the security-critical accept
path; the reroute touches the hot spawn path).  They stay LOCAL (3432ceb8fb8)
pending that -- do not land a partial connect fix while the workload still
stalls at ~0 throughput.

## The thesis result we HAVE (unchanged): efficiency confirmed, throughput pending
Fork: -S ~808-814k tps, -N ~137-144k tps @384/768; fork ctx-switch ~1.75-2.66
MILLION/s.  tps fiber ctx-switch ~14-16k/s (~130x fewer) -- but NOT yet at equal
throughput (tps throughput still stalled), so the "same TPS, 150x fewer switches"
claim remains UNPROVEN pending the wake-throughput unblock.

## Blocker chain (updated)
unpooled-ENOSYS -> HammerDB-monitor -> io_method=xtc-futex -> pooled-carrier-
starve -> accept-serialization (FIXED) -> socket leak (FIXED) -> eventfd-leak
(not real) -> SSLRequest negotiation latency (FIXED, A) -> spawn-send drop
(FIXED, reroute) -> idle->active wake throughput (NEW, next).

## Status
Connection drops fixed + metal-validated.  Wake-throughput layer isolated as the
next blocker.  A + reroute local (3432ceb8fb8) pending wake-layer + review.
All MY EC2 torn down + verified (xtc-numa-bench us-east-2 is another owner's).
