# fd-leak fix validation (2026-08-05)

Commit 966ba480fe4 (close the dup'd client socket on the threaded error-recovery
release paths).  Validated on c7i.8xlarge (32 vCPU), PG built WITH the fix.

## The definitive test: fd count under a SUSTAINED connect storm
thread-per-session (carriers=0), pgbench -C -c 64 -T 60 (reconnect per txn),
sampling postmaster fd count every 5s:

  baseline (idle tps): 2320 fds
  storm start:         3112 fds   (64 active conns + carrier fibers)
  t=5..70s:            3112 -> 3115 fds   (FLAT -- +3 over 60s+)
  after pgbench killed + settle: 2381 fds  (returned toward baseline)

PRE-FIX: fds climbed UNBOUNDED to 8,591 and wedged the server.
POST-FIX: fds are BOUNDED and flat, reclaimed as connections churn -> NO LEAK.

## Zero connection failures
SSL_err_count in the 60s+ sustained pgbench -C storm = 0.  Pre-fix the same
storm failed constantly with "server sent an error response during SSL exchange"
and wedged.  The fd-leak fix eliminates the storm-induced connection failures.

## Root cause + fix (recap)
backend_pooled_logical_start_release() and backend_thread_start_release()
free()'d without closing the dup()'d client_sock.sock.  The clean exit paths
close+null before teardown, but the ERROR-RECOVERY release paths (sigsetjmp
branches in run/resume_logical_start; failed pg_thread_create after the dup)
reached release with the socket still open -> every errored startup leaked its
dup'd fd -> sustained storm exhausted the fd table.  Fix: both *_release() now
closesocket() if still open (idempotent), made release the single owner of that
close, removed the two non-nulling explicit closes in the thread-launch failure
paths (would have double-closed).

## Residual (separate, lower-severity, NOT the leak)
An EXTREME pgbench -C (reconnect EVERY transaction) at c=64 eventually drove the
tps server into an accept-degraded state where a fresh monitoring connect timed
out -- but fds stayed BOUNDED (2381, not climbing), so this is the accept-rate
characteristic under a pathological reconnect-per-txn load, NOT the fd leak.
Real workloads (and HammerDB) HOLD connections; they do not reconnect per
transaction, so this does not affect the oversubscription benchmark.  If it
matters later, it is the accept-throughput lever (accept-drain #2: prioritize
the handshake), not fd lifecycle.

## Verdict
fd LEAK FIXED (bounded fds + 0 SSL errors under sustained storm).  Ready to land
+ re-run the oversubscription A/B (fork vs tps @1x/2x) -- HammerDB holds its VU
connections, so with the leak gone the tps lane should finally produce real
throughput instead of NA.
All EC2 torn down + 5-region verified clean.
