# Threaded connection-storm diagnosis (2026-08-05) -- NOT slot lifecycle, NOT TLS

## Method
Built PG (origin/xtc) + libxtc v1.32.0 on a c7i.4xlarge (16 vCPU), instrumented
AssignPostmasterChildSlot to WARN with pool-size + active-B_BACKEND count on an
empty freelist, and ran connection storms against a pooled threaded server.

## Results (the evidence)
  test                                              ok    fail
  50 concurrent, hold 0.2s   (pooled, 16 carriers)  50    0
  150 concurrent, instant    (pooled, 16 carriers)  150   0
  300 concurrent, hold 2-3s  (pooled, 16 carriers)  96    204
  300 concurrent, hold 2s, STAGGERED 20/0.1s (16c)  160   140
  300 concurrent, hold 2s    (THREAD-PER-SESSION,   300   0
                              pooled_protocol_carriers=0)

Key observations:
- The XTCDIAG slot-exhaust WARNING NEVER fired.  "too many clients" count = 0.
  So it is NOT PMChild slot exhaustion (the earlier ab5 hypothesis was wrong --
  the slot pool 2*(MaxConnections+wal_senders) is not the limit here).
- The failing clients produced NO stderr and the SERVER logged NO error: the
  connections TIME OUT / are dropped while waiting to be serviced, not rejected.
- Staggering the connects barely helped (160 vs 96 ok): it is not a
  thundering-herd/backlog problem.
- THREAD-PER-SESSION (carriers=0) handled the SAME 300x2s storm with 300 ok, 0
  fail.

## Root cause (evidence-based)
It is the POOLED scheduler's fundamental concurrency limit, not a bug:
pooled_protocol_carriers=N means at most ~N sessions can be ACTIVELY RUNNING at
once -- and a connecting session needs a carrier to run its startup/auth/first
command.  With N=16 carriers and 300 sessions that each hold their carrier for
2s of work, connection establishment STARVES: sessions queue for a carrier, do
not get one within the client's connect/auth timeout, and fail.  The earlier
"error response during SSL exchange" on metal (ab5) was the same starvation on
the 192-carrier metal config hit by 384+ simultaneous HammerDB VUs (a dropped/
reset connection during the SSLRequest read surfaces as that libpq message).

THREAD-PER-SESSION has no such ceiling: each backend is its own libxtc fiber,
xtc_exec schedules all 300 across ~ncore loops, and every connection is
serviced.  This is the direct confirmation of the whole thesis: thread-per-
session (real fibers) is the libxtc-native model that HANDLES oversubscription;
the pooled scheduler is the deviation that cannot (its carrier count caps active
concurrency).

## Consequence for the benchmark + direction
- The oversubscription A/B must use THREAD-PER-SESSION (carriers=0) as the mt
  lane, NOT pooled.  Pooled is expected to carrier-starve under oversubscription
  by design and should be dropped from (or clearly labelled in) the oversub run.
- No slot-lifecycle "fix" is warranted -- slot lifecycle is correct.  The pooled
  scheduler's active-concurrency cap is inherent to that design.
- This strengthens the earlier conclusion (MULTITHREADED_OVERSUBSCRIPTION_AB.md):
  lean into thread-per-session-as-fibers; it is the mode that scales work across
  cores in-process the way the thesis predicts.  The remaining question the A/B
  answers: at 2x oversubscription does thread-per-session BEAT fork on
  throughput / RSS / context-switches?

## Next
Re-run the oversubscription A/B: fork vs mt-thread-per-session (carriers=0) at 1x
and 2x cores, io_method=sync both, metrics srv_tpm + RSS + context-switch rate +
p95/p99.  Drop the pooled lane (or keep only as a labelled "pooled starves"
contrast at 1x).  The connection storm is a non-issue for thread-per-session
(300x2s = 300 ok proven), so HammerDB's simultaneous-VU-open will not fail it.
