# eventfd "leak" RE-DIAGNOSED: NOT a leak (2026-08-06)

## The controlled test overturns the ab8 leak hypothesis
On a c7i.12xlarge (48 vCPU), tps server, current HEAD (socket-close + accept-drain
fixes landed), sampling postmaster fd count BY TYPE across bursts:

  BASELINE (idle tps, max_connections=512): total=1344, 1172 [eventfd]
    (1172 = ~2*TotalProcs static per-PGPROC sem_wake_fd+interrupt_wake_fd,
     created ONCE in InitProcGlobal -- NOT per-session, NOT a leak.)
  6 waves x 384 simultaneous connects (all succeed): total FLAT 1345, eventfd FLAT 1172.
  1000 simultaneous connects with pg_sleep(1) -> 520 ok / 480 FAIL:
     pre eventfd=1172, post=1172, settled=1172  -- ZERO growth despite 480 failures.

CONCLUSION: there is NO fd leak.  480 connection FAILURES produced no eventfd
growth at all.  The ab8 metal "8,532 eventfds" was a TRANSIENT in-flight snapshot
(connections mid-handling during a live storm), NOT leaked fds -- I mis-read a
point-in-time count as a leak.  The socket-close fix (966ba480fe4) + accept-drain
(37285c8daba) are correct and sufficient for fd lifecycle.

## What the connection FAILURES actually are (not a leak)
1000 simultaneous (or 288/1000 even staggered) connects each holding pg_sleep(1)
overwhelm a ~48-carrier tps pool: 1000 sessions x 1s work / 48 carriers cannot
all complete within the client's 10s timeout, so the excess time out.  That is
the CARRIER-THROUGHPUT / accept-rate ceiling under held work -- expected and
bounded, not a resource leak.

## The A/B-relevant fact: persistent-connection workloads WORK
pgbench -S -c 384 (PERSISTENT connections -- connect once, then many quick
queries, exactly HammerDB's steady-state pattern) COMPLETES, server stays
healthy, fds bounded (3610 for 384 active, settles).  Real HammerDB holds its VU
connections for the whole run; it does not reconnect per transaction, so the
carrier-throughput ceiling (which only bites reconnect-churn / instantaneous
mega-bursts) does not apply to its steady state.

## Why ab8's HammerDB still NA'd (the actual, narrower cause)
HammerDB opens all 384 VUs at ONE instant (Tcl threads), a tighter burst than
psql can produce.  The accept-drain admits a burst per ServerLoop pass, but 384
truly-instantaneous handshakes on the 384-vCPU metal box can still outrun the
per-fiber handshake completion, timing out some VUs at CONNECT.  This is the
accept/handshake-latency ceiling (fix-lever #2 from ACCEPT_BOTTLENECK.md:
prioritize the handshake), NOT a leak and NOT slot/carrier exhaustion.

## Path to the A/B (no more code fixes needed for correctness)
The runtime is CORRECT (no leak, persistent workloads work).  To get HammerDB's
initial VU-connect burst through, make the BENCHMARK connect robustly rather than
chase a sub-ms handshake race:
  - HammerDB: stagger VU creation, OR set a longer connect/login timeout, OR let
    it retry the initial connect.  Its steady-state (persistent conns) is fine.
  - Simplest for the A/B: drive the mt lane with a workload that holds
    connections (pgbench -S/-N with persistent conns, already the pgbench path),
    which we KNOW works at 384; use HammerDB with staggered VU start.

## Status
No leak (re-diagnosed).  accept-drain + socket-close fixes stand (correct).
Fork baseline: @384 ~2.4M srv_tpm, ~412k ctx-sw/s.  The A/B is runnable with a
connect-robust driver; the remaining item is a BENCHMARK-DRIVER change (stagger/
retry HammerDB VU connect), not a runtime bug.  All EC2 torn down + verified.
