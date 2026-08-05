# ab5 connection-storm failure -- root cause is NOT TLS (2026-08-05)

## What you observed
mt (thread-per-session AND pooled) fail ~47% of connections under a storm
(100 simultaneous connects -> 47 fail) with libpq error:
  "server sent an error response during SSL exchange"
This blocked the oversubscription A/B (HammerDB opens all VUs at once = a storm).

## Root cause (traced to source -- it is NOT TLS)
SSL is OFF (no ssl=on; pg_hba is `trust`).  libpq sends an SSLRequest at connect
and expects one byte: 'S' (use SSL), 'N' (no SSL), or 'E' (ErrorResponse).  It
reports "error response during SSL exchange" precisely when it gets 'E'.

The server sends 'E' = ereport(FATAL, "sorry, too many clients already") --
backend_startup.c:384, reached when cac == CAC_TOOMANY, set in postmaster.c:4151
when AssignPostmasterChildSlot(B_BACKEND) returns NULL, i.e. the PMChild slot
pool is EXHAUSTED.  That error arrives DURING the SSLRequest negotiation phase,
so libpq mislabels it "SSL exchange" -- but no TLS/SSL code runs at all (the 'N'
path never touches be_tls_* / secure_open_server).

The B_BACKEND slot pool is 2*(MaxConnections+max_wal_senders) ~= 2100 slots
(pmchild.c:196), FAR more than 100.  So the pool is NOT undersized -- slots are
being HELD/leaked by the storm's failing connections in threaded mode faster
than they are released (ReleasePostmasterChildSlot).  Under a burst, assigned
slots pile up (session assigned a slot at accept, then stalls/fails during
startup handshake before releasing) and the pool drains -> CAC_TOOMANY ->
'E' -> the client's "SSL exchange" error.

## Therefore: the libxtc TLS wrappers do NOT fix this
Swapping be_tls_* -> xtc_tls_* cannot change a plaintext "too many clients"
rejection.  The TLS swap is also (a) blocked on libxtc lacking the SNI
context-selection callback (see libxtc-tls-sni-transport-request.md) and (b) a
~2500-line security-critical rewrite -- neither of which touches the slot
exhaustion.  Implementing it here would be effort against the evidence.

## The actual fix (threaded connection-storm slot lifecycle)
The real work is in the threaded accept/startup path:
1. CONFIRM the leak: instrument AssignPostmasterChildSlot /
   ReleasePostmasterChildSlot counts under the storm -- are assigned slots
   released promptly when a threaded startup handshake fails or when the client
   just probes (SSLRequest then closes)?  A dead-end/failed session must release
   its B_BACKEND slot immediately, not hold it.
2. Likely culprits to trace in threaded mode:
   - a session assigned a slot at accept that then blocks waiting for a carrier
     lease (pooled) or a fiber spawn (tps) holds the slot for the whole stall;
     under a burst that is every slot.
   - failed/aborted startup on a carrier fiber may defer slot release to a
     teardown path that lags under load.
   - the accept rate vs carrier/fiber bring-up rate: if accept assigns slots
     faster than carriers can service them, the pool fills transiently.
3. Compare to fork: the postmaster forks a dedicated process per accept
   immediately, so a slot is backed by a running process at once -- no stall
   window.  The threaded path needs the equivalent: do not hold a B_BACKEND slot
   across a bring-up/handshake stall, or size/backpressure the accept loop to
   the carrier service rate.

## Impact on the oversubscription thesis
The A/B is BLOCKED until this is fixed -- the storm fails connections before
they do work, so no steady-state throughput can be measured on the mt lanes.
This connection-storm robustness bug is HIGHER priority than the oversubscription
number: it would break any real workload with connection bursts, and it must be
fixed for the mt lanes to even be benchmarkable.  Fork datapoint captured:
@384 clients srv_tpm=2,447,257, context-switches=416,806/s (the kernel-scheduling
cost the thesis predicts fibers should undercut -- once we can measure them).

## Harness mitigation for the NEXT A/B (so a residual storm does not mask results)
Stagger HammerDB VU creation / add a connect-retry-with-backoff so a transient
CAC_TOOMANY does not kill a VU, AND fix the underlying slot lifecycle.  But the
slot fix is the real deliverable; the stagger only stops the benchmark from
mis-measuring.
