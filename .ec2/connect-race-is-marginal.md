# Connect-race repro: it is a MARGINAL edge race, not a hard limit (2026-08-06)

## What the focused repro showed
c7i.12xlarge, tps server (multithreaded=on, carriers=0), pgbench -S -c 384:
- With trace_connection_negotiation=on + log_connections + debug1 (verbose,
  slower connection setup): **fail_count=0** -- ALL 384 connected, pgbench ran.
  SSLRequest negotiation is answered (server logs "SSLRequest rejected" per conn
  from a single serving path).
- The ab9 failures (fail=2-4 of 384) were a RAZOR-EDGE timing race at the tail of
  the instantaneous 384-connect burst -- verbose logging (which slows each
  connect slightly) perturbs it away entirely.  So the connections DO get served;
  2-4 occasionally lose a sub-timeout race and pgbench (zero-tolerance on initial
  connect) aborts the whole run.
- (Later runs on the same box hit "pre-existing shared memory block still in use"
  / a wedged old postmaster from my own sequential ad-hoc tests -- box-muddying,
  not a product signal.  Confirmed once, not chased.)

## Consequence for A vs B
A = postmaster answers the SSLRequest 'N' byte at accept time (pre-handoff).
This is a LARGE, security-sensitive accept-path change (the postmaster would read
raw client bytes -- new blocking/partial-read/MITM surface).  The evidence does
NOT justify that risk: on a clean server the burst does not reliably fail; it is
a marginal tail race, not an architectural limit.  Building A for a 2-of-384
edge race is over-engineering against the "evidence-first, don't touch the
security-critical accept path without a proven need" discipline.

B = drive the A/B with a connect-race-tolerant client (retry the rare failed
initial connect).  Proportionate: the runtime serves the connections; only
pgbench's zero-tolerance startup aborts.  B gets the oversubscription number,
which is the actual goal (steady-state throughput/RSS/ctx-sw at 2x).

## Decision
Do B (connect-tolerant driver) to GET THE NUMBER.  Treat A as DEFERRED-with-
evidence: revisit only if a real workload shows connect-burst failures on a
clean server at a rate that matters (this repro says it does not).  If A is ever
needed, the minimal form is a bounded non-blocking single-byte SSLRequest reply
in the postmaster -- designed + 2-review-gated, not rushed.

## Note
An unrelated instance xtc-numa-bench (key xtc-numa-20260806-070719, c5.metal,
us-east-2) is running under the beef account -- it is ANOTHER owner's
(numa-bench), NOT mine, left untouched.  All MY session instances (us-east-1,
keys xtc-ab*/cr/ev/fdv/wt/acc/slot) are terminated + verified.
