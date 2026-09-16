# Wake/read tracing: the client-read path is PROVEN healthy. The wedge is downstream of the read.

Date: 2026-09-16. Followed the plan from PARK_TRACE_FINDINGS: trace the wake classification and the
park generation. The trace went somewhere more useful -- it eliminated the whole read path.

## First, a structural correction: I instrumented the wrong function
I added the wake trace to `PgSessionStagingWaitProtocolRead` (the `transport_wait_events` /
`park_spec->generation` classifier) and got **0 traces** from a wedged run. Reason: my own P-A1 fix
(`PostgresRunSession`, committed earlier) routes the fiber path to plain `PgSessionRun`, which parks via
`pq_getbyte -> secure_read -> WaitEventSetWait` and **never enters the stackless staging**. So the
`transport_wait_events` / generation-staleness hypothesis is **not applicable to the fiber path at all** --
that classifier belongs to the pooled scheduler. Hypothesis retired, not merely unproven.

## What the correct instrumentation shows (`PG_XTC_SREAD_TRACE=1`)
Traced the real fiber park site, `secure_read`'s retry loop:

**The wait always gets exactly what it asked for.** 145,528 wakes:
```
145,412   waitfor=0x2 (WL_SOCKET_READABLE)  ->  ev=0x2 (WL_SOCKET_READABLE)  pos=0 (client socket)
    116   waitfor=0x2                       ->  ev=0x1 (WL_LATCH_SET)        pos=1 (latch)
```
No mismatch, no spurious class, no zero-return timeout.

**And the reads themselves succeed.** 32,885 read results in a run that wedged hard (176 zero-tps
intervals):
```
  ok     : 25,883  (78.7%)   -- n=8192 (11,352x), n=76, n=64, n=12, n=10 ...
  eagain :  7,002  (21.3%)   -- n=-1 errno=11, the normal "not ready yet, go wait" path
  other  :      0
```
A 79/21 success-to-EAGAIN ratio is exactly what a healthy blocking-read emulation looks like.

## Conclusion: the read path is not the bug
`WaitEventSetWait` + the epoll registration + `secure_read` + `recv()` all behave correctly during the
wedge. Data arrives, is signalled, and **is consumed**. Combined with the earlier park trace (the fiber
wakes ~800x/second and harvests a real event), the entire client-input path is exonerated.

So the stuck sessions are not failing to *read* -- they read their command and then never *answer* it.
`pg_stat_activity` showing them `idle in transaction` on `ClientRead` is the state *after* they finished
one command and are waiting for the next, while the client waits for a reply to a command whose
processing never completed. The `Recv-Q > 0` sockets I found earlier are consistent with that: the client
pipelined the next request while the previous one's reply never came.

## Cumulative elimination list (all by direct measurement, not inference)
- libxtc: mailbox wake (v1.47.0 fix), cross-thread send, io_uring specifics, ring overflow, work-stealing
  -- and the whole IO backend, since **epoll wedges too, sooner** (45 s vs 90 s).
- PG wait layer: `WaitEventSetWaitBlock` fiber branch, epoll registration, the `event->events` mirror,
  `ModifyWaitEvent`'s fast path.
- PG read layer: `secure_read`'s retry loop, `secure_raw_read`, `recv()`.
- Locking: lost LWLock wake (waiters armed with `eventfd-count: 0` = no wake ever attempted), buffer
  content locks (`WAKE_IN_PROGRESS=0`, gate open), the deadlock detector (a deliberate 2-session deadlock
  IS caught), row contention (fork sustains ~3,060 tps on the identical workload).
- Pooled machinery: `transport_wait_events` / park-generation staleness -- not on this code path.

## Next: instrument the QUERY side, not the transport
The remaining candidates are between "command read" and "reply written":
1. Query execution blocking on something not yet traced -- most likely the WAL/commit path, since the
   symptom is write-heavy and read-only never wedges.
2. The reply write path (`secure_write` / `internal_flush`), which has its own
   `WL_SOCKET_WRITEABLE` wait loop at be-secure.c:413 -- structurally identical to the read loop I just
   cleared, and *not yet traced*. A session blocked writing its reply would look exactly like this: read
   succeeds, no answer emitted, client waits forever.

(2) is cheap to test and fits the evidence best (write-heavy only, reads fine), so it is next. I will
trace the write loop with the same macro shape before touching anything else.

## Methodology note
Three hypotheses retired today (epoll desync, `transport_wait_events` staleness, read-path stall), each
killed by measurement rather than argument. The pattern that works: instrument the exact code path the
running configuration takes -- verified by "did my trace fire at all?" -- and always decode numeric
fields against their constant table before interpreting them.
