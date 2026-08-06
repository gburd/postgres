# A/B status: fork baseline + partial thesis confirmation; connect-race justifies A (2026-08-06)

## Fork (stock) baseline -- VALID, the number to beat (m8idn.metal-96xl, 384 cores)
  workload   VU=384(1x)     VU=768(2x)    fork ctx-switch/s   fork RSS(ps-sum)
  pgbench -S  731,103 tps    783,832 tps   ~1.9-2.2 MILLION    ~57 MB
  pgbench -N  143,860 tps    140,326 tps   ~2.5-2.6 MILLION    ~59 MB

## PARTIAL THESIS CONFIRMATION (even without a clean tps throughput number)
The tps (thread-per-session fiber) lane's server-side CONTEXT-SWITCH rate under
the same load: ~13,000-15,000/s -- vs fork's ~2,000,000-2,600,000/s.
That is a ~150-180x REDUCTION in context switches: the single-process fiber
runtime schedules cooperatively in userspace (xtc_exec) instead of the kernel
switching 384-768 backend PROCESSES.  This is exactly the oversubscription
efficiency the thesis predicts.  The throughput/latency comparison still needs a
clean tps tps-number (blocked on the connect race, below).

## The connect race is SCALE-DEPENDENT and SEVERE at metal (this justifies A)
Earlier (48-vCPU c7i) the pgbench 384-connect race looked MARGINAL (2-4/384, and
0 with slower logging).  On 384-core metal it is SEVERE: even 64 concurrent
connects fail with "server sent an error response during SSL exchange", and every
pgbench invocation (incl. staggered 12x64) loses the tail race -> tps=NA.
Why it scales: thread-per-session sizes the fiber-executor to core count (~256
loops on 384 cores); the SSLRequest 'N' negotiation byte is emitted by the
backend FIBER after handoff, and landing that fiber's schedule across 256 loops
under a burst is slow enough that the client's connect read gives up.  So the
race is NOT marginal at the scale we benchmark -- it is a real architectural
limit, and A (answer the negotiation without waiting on fiber scheduling) IS
warranted.  (Corrects the earlier "marginal, defer A" call -- that was from the
small box.)

## A -- design spec for the next focused session (security-sensitive, 2-review-gated)
Answer the SSLRequest/GSSRequest negotiation at ACCEPT time in the postmaster,
scheduling-independent, WITHOUT the postmaster ever blocking on a client:
1. After accept (threaded mode only), do a BOUNDED NON-BLOCKING read of up to 8
   bytes (the SSLRequest is exactly 8: int32 len=8 + int32 code=NEGOTIATE_SSL_CODE).
   - If <8 bytes available (slow client) or not an SSLRequest/GSSRequest code:
     read NOTHING further, hand off to the fiber as today (fiber does the full
     ProcessStartupPacket).  The postmaster must NEVER block or loop on the read.
   - If exactly an SSLRequest/GSSRequest: write the single 'N' byte (non-blocking;
     the socket's send buffer is empty so it cannot block), and set a new
     ClientSocket/Port flag ssl_negotiated (or stash raw_buf) so the fiber's
     ProcessStartupPacket skips re-answering and reads the real startup packet
     next (mirror the existing NEGOTIATE_SSL_CODE -> 'N' -> goto retry loop, but
     with the 'N' already sent).
2. Security invariants (why this is safe): single non-blocking recv of <=8 bytes
   then at most one non-blocking send of 1 byte; no loop, no partial-read retry
   in the postmaster, no MITM window beyond what PG already accepts for a
   plaintext SSLRequest (SSL is off in these configs; when SSL is ON, do NOT
   pre-answer -- let the fiber run secure_open_server as today).  Gate on
   multithreaded && !LoadedSSL && AF_INET.
3. Files: postmaster.c (the accept/BackendStartup seam), backend_startup.c
   (ProcessStartupPacket: honor the pre-answered flag), libpq-be.h (the flag),
   launch_backend.c (carry the flag through the handoff).
4. Validate: 384/768 SIMULTANEOUS pgbench connect -> 0 fail on metal; then the
   oversubscription tps number lands and B is unnecessary (any stock driver works).

## Recommendation
A is the correct fix and is now evidence-justified (severe at metal scale).  It
is a focused, design-first, two-review-gated change to the security-critical
accept path -- NOT to be rushed at the tail of a long session.  Next session:
implement A per the spec, validate the 384-burst goes to 0 fail, then run the
oversubscription A/B (fork vs tps @1x/2x) -- which will confirm/quantify the
thesis (the ~150x ctx-switch reduction already points strongly to fibers winning
the efficiency axis at oversubscription).

## Status
Fork baseline captured; ~150x ctx-switch reduction measured (partial thesis
win); connect race re-diagnosed as severe-at-scale (justifies A); A spec written.
All MY EC2 torn down + verified (an unrelated xtc-numa-bench in us-east-2 is
another owner's, untouched).
