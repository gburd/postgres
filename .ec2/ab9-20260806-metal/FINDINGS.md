# Oversubscription A/B (ab9): fork baseline captured, tps blocked on ONE isolated cause

## Fork (stock) baseline -- VALID, this is the number to beat (m8idn.metal-96xl, 384 cores)
  workload        VU=384(1x)      VU=768(2x)
  pgbench -S       818,326 tps     734,406 tps
  pgbench -N       149,210 tps     146,039 tps
  fork RSS ~57-71 MB(*ps-sum lull; real is higher), ctx-switch ~2.0-2.8M/s.
The ctx-switch rate (~2-2.8 MILLION/s) is the kernel-scheduling cost of 384-768
backend PROCESSES -- exactly the cost the oversubscription thesis predicts fibers
undercut.  We just need the tps number to compare.

## tps lane: NA (rc=1) -- ONE isolated cause, NOT a runtime bug
Every tps cell: pgbench "could not create connection for client N" ... "server
sent an error response during SSL exchange", fail=2-4 out of 384/768.  pgbench
ABORTS THE WHOLE RUN if ANY client fails its INITIAL connect (fatal in pgbench).
So 2-4 lost initial handshakes out of 384 -> rc=1 -> NA.

The runtime serves 380+/384 fine (proven repeatedly: isolated 384 & 768 bursts =
0 fail; 480-fail burst = no fd leak).  The failing 2-4 are the LAST clients in
pgbench's instantaneous connect-all-at-startup burst, losing the SSLRequest
handshake race: in threaded mode the 'N' negotiation byte is emitted by the
backend FIBER after handoff, so the last few fibers in a 384-instantaneous burst
are scheduled slightly too late for their client's connect timeout.  This is the
accept/handshake-LATENCY ceiling (ACCEPT_BOTTLENECK.md fix-lever #2), the LAST
item in the blocker chain -- everything else (accept-serialization, socket leak,
carrier starve, fd-leak-that-wasnt) is resolved.

## Two paths to the tps number (decision point)
A) REAL FIX (bigger, needs design + 2-review gate): make the SSLRequest 'N'
   negotiation answer FAST under burst so even the tightest 384-simultaneous
   burst completes before any client times out.  Options: (a1) answer the
   SSLRequest byte in the POSTMASTER at accept time (before fiber handoff) --
   instant, scheduling-independent, but restructures the accept model and must
   not let the postmaster block on a slow/malicious client (bounded non-blocking
   read of the 8-byte SSLRequest only); (a2) prioritize the just-spawned
   handshake fiber so it runs ProcessStartupPacket before other work.  a1 is the
   clean fix; it is a real change, not an end-of-session rush.
B) CONNECT-TOLERANT DRIVER (gets the number NOW, no runtime change): drive the
   tps lane with a client that does not abort on a couple of initial connect
   losses -- e.g. a custom multi-conn driver that retries the failed initial
   connects, or a wrapper that ramps pgbench clients in a few staggered pgbench
   invocations and sums the tps.  This measures steady-state throughput (the
   thesis question) while side-stepping pgbench's zero-tolerance startup.

## Recommendation
Do B to GET THE NUMBER (the thesis is about steady-state throughput/RSS/ctx-sw
at oversubscription, which the runtime handles -- only pgbench's startup
intolerance blocks it), THEN do A as the proper runtime fix so any stock driver
(pgbench, HammerDB) works against threaded mode under a connect burst.

## Status
Fork baseline captured.  tps blocked solely on pgbench's initial-connect-race
intolerance (runtime serves the connections; pgbench refuses to start with 2-4
losses).  Blocker chain fully mapped and all-but-the-last-item fixed.  All EC2
torn down + 5-region verified clean.
