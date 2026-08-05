# Threaded connection-accept bottleneck (2026-08-05) -- the real blocker

## Reproduced (metal m8idn.metal-96xl, 384 vCPU)
384 SIMULTANEOUS trivial connects (`select 1`, no work held) to a
thread-per-session (carriers=0) server:  66 ok / 318 fail, all with libpq
"server sent an error response during SSL exchange", server log SILENT.
Scales with burst size: on a 16-vCPU box 150 instant connects = 150 ok; on
384-vCPU metal 384 simultaneous = 318 fail.  HammerDB (opens all VUs at once)
therefore fails every mt cell (both tps and pooled) at 384/768 VUs.

## Root cause (traced to source)
It is NOT slot exhaustion (SLOT_DIAGNOSIS.md), NOT carrier starvation (these
connects hold no carrier), NOT TLS (SSL is off).  It is accept + early-handshake
LATENCY under burst:

- The postmaster accepts ONE connection per ServerLoop iteration per listen
  socket (postmaster.c ~1949: for each WL_SOCKET_ACCEPT event -> AcceptConnection
  does a single accept() -> BackendStartup).  The loop is serial.
- The SSLRequest negotiation ('S'/'N' byte) + startup packet is processed in the
  BACKEND/FIBER after handoff (BackendInitialize -> ProcessStartupPacket in
  backend_startup.c, run by BackendMainWithStartupData), NOT in the postmaster.
- So each client, having sent SSLRequest, must wait for (a) the postmaster to
  reach its accept in the serial loop AND (b) its fiber/session to be scheduled
  and run ProcessStartupPacket to emit the 'N' byte.  Under 384 simultaneous
  SYNs+SSLRequests, that latency exceeds the client's connect/SSL-negotiation
  timeout for most of them; the client aborts, which libpq reports as "error
  response during SSL exchange" (a failed negotiation read).  The server logs
  nothing because the connection dies before/at the earliest backend step.
- Fork mode does not hit this: the postmaster forks-and-forgets per accept and
  the forked child immediately answers its own SSLRequest -- fast, and the
  accept loop's per-iteration work is minimal.

## The fix (accept-path throughput in threaded mode)
Two independent levers; do the first, measure, add the second if needed:
1. DRAIN THE ACCEPT BACKLOG PER ITERATION: on a WL_SOCKET_ACCEPT event, accept()
   in a loop until EAGAIN (bounded by a cap) instead of one-per-iteration, so a
   burst is admitted quickly and the SSLRequest answers are not serialized behind
   the whole ServerLoop.  The listen socket is non-blocking, so a drain loop is
   safe.  This is the smallest, highest-leverage change (postmaster.c accept
   handling).
2. Ensure the newly-launched session's fiber is scheduled promptly to run
   ProcessStartupPacket (the 'N' write): if fiber bring-up lags under burst,
   answering SSLRequest before the client times out needs the accept->fiber
   path to prioritize the handshake.  Measure whether (1) alone closes it first.

Also consider: raise the client-side/AuthenticationTimeout tolerance is NOT the
fix (masks it); the accept drain is.

## Consequence for the benchmark
The oversubscription A/B is BLOCKED on this: HammerDB's simultaneous VU open
fails the mt lanes before any work runs (which is why every mt HammerDB cell this
session was NA/collapsed -- the accept burst, not the scheduler).  Fix the accept
drain, THEN the oversubscription thesis (tps vs fork at 2x) becomes measurable.

## Fork baseline captured (the valid lane, both runs consistent)
@384 (1x cores): srv_tpm ~2.37-2.45M, RSS ~59MB(*), context-switches ~407-417k/s.
(* the RSS 59MB reading is suspect -- ps rss sampling caught a lull; re-measure
with a mid-window RSS snapshot once the mt lane runs.)  The ~410k ctx-switch/s is
the kernel-scheduling cost the oversubscription thesis predicts fibers undercut.

## Next
Implement fix (1) -- accept-backlog drain per ServerLoop iteration -- as a
scoped, reviewed change; re-verify the 384-burst goes to ~0 fail; then re-run the
oversubscription A/B (fork vs tps @1x/2x).
