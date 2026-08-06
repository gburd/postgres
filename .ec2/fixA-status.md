# Fix A (accept-time SSLRequest) -- real improvement, but the burst has a 2nd layer (2026-08-06)

Commit 4af3e0f0084 (LOCAL, not pushed): pg_prenegotiate_ssl_request() answers the
SSLRequest 'N' in the postmaster at accept time (bounded non-blocking peek + 1-byte
send; gated multithreaded && !LoadedSSL && TCP; ClientSocket.ssl_negotiated ->
Port.ssl_prenegotiated -> ProcessStartupPacket ssl_done).

## What A fixed (validated on m8idn.metal-96xl, 384 cores)
- Single + 10 sequential connects: clean (select 1 = 1).  A does NOT break normal
  connections.
- The burst error CHANGED from "server sent an error response during SSL exchange"
  to "server closed the connection unexpectedly".  So A eliminated the
  SSLRequest-negotiation-latency failure (the 'N' is now answered instantly at
  accept, scheduling-independent) -- that layer is fixed.

## What A did NOT fix -- a SECOND burst failure layer
pgbench -S/-N -c 384/768 (all-at-once connect) STILL fails (tps=NA, tries=6), now
with "server closed the connection unexpectedly" (server logs nothing).  So under
the 384-instantaneous burst, after the 'N' is answered, some connections are
closed during the subsequent startup/fiber-bringup.  Single & sequential are
fine, so it is burst-specific: the fiber bring-up for 384 simultaneous new
sessions still can't keep up, and some connections drop after negotiation.
Hypotheses to chase (next session, evidence-first):
  - the just-spawned backend fiber's ProcessStartupPacket reads the startup
    packet but the pq buffer / raw_buf state after the postmaster's raw recv() of
    the SSLRequest is subtly off (the postmaster consumed 8 bytes from the shared
    file description; verify the fiber's first pq_recvbuf sees the startup packet
    cleanly and nothing was left/duplicated);
  - or the accept-drain admits 384 at once but the fiber-spawn/registration rate
    can't match, and excess sessions are closed (a bring-up backpressure limit,
    NOT negotiation) -- would be the real remaining ceiling.

## Assessment
A is a genuine, correct improvement (eliminates the SSL-exchange latency error,
normal connects unaffected) and should land AFTER: (1) the 2nd-layer
"connection closed under burst" is diagnosed + fixed, and (2) the two-review gate
(it touches the security-critical accept path -- postmaster raw recv/send).  Do
NOT land A alone: it half-solves the burst and the residual close needs
understanding first.  A stays LOCAL (4af3e0f0084) pending that.

## The thesis result we DO have (unchanged, strong)
Fork baseline: pgbench -S ~707-819k tps, -N ~134-142k tps @384/768; fork
ctx-switch ~2.0-2.6 MILLION/s.  tps (fiber) ctx-switch ~15-17k/s under the same
load = ~130-170x fewer context switches.  The efficiency axis of the
oversubscription thesis is confirmed; the throughput number still needs the burst
fully unblocked (A layer 1 done, layer 2 + review remain).

## Status
A layer-1 done (local, unproven-for-landing).  Burst layer-2 = next diagnosis.
All MY EC2 torn down + verified (xtc-numa-bench in us-east-2 is another owner's).

## Layer-2 lead (source analysis, 2026-08-06)
Ruled OUT: ProcessSSLStartup (runs before ProcessStartupPacket, does pq_peekbyte)
mis-handling the post-'N' bytes -- after 'N' the client's next byte is the
startup length high-byte (~0x00), != 0x16, so ProcessSSLStartup returns STATUS_OK
correctly.  Not the bug.

Remaining hypothesis for "server closed the connection unexpectedly" (no server
log, burst-only, single/sequential fine): the backend fiber exits cleanly
(no error logged) but closes the socket during startup under the 384-instant
burst -- pointing at a fiber BRING-UP backpressure/limit (too many simultaneous
new sessions spawned; some torn down), NOT the SSL negotiation (which A fixed).
Next session, INSTRUMENT a failing connection on-box: log in the tps
backend-fiber startup path (BackendInitialize/ProcessStartupPacket entry + the
exit path) to see whether the fiber even starts for the closed connections, or
is rejected/torn down at spawn under the burst.  If bring-up backpressure: that
is the true remaining oversubscription-connect ceiling and needs its own fix
(rate-limit/queue the fiber spawns, or raise the spawn concurrency).

## Path to the tps NUMBER without fully solving layer-2 (pragmatic)
Since single + sequential connects are clean, the tps STEADY-STATE throughput can
be measured by ESTABLISHING the connections gradually (a driver that opens N
persistent conns one/few at a time with retry, then runs the query loop and
measures server-side xact/s) -- avoiding the instantaneous 384-burst entirely.
That gets the throughput comparison (the thesis question) while layer-2 (the
instantaneous-burst ceiling) is fixed separately.  This is the cleanest next
action to finally quantify tps-vs-fork throughput at oversubscription.
