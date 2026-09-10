# Pooled scheduler: sessions in excess of carriers are STARVED INDEFINITELY (not slow — starved)

Date: 2026-09-10
HEAD: `66f07e82b9` + libxtc **v1.43.0** (`b977d21`, pristine — verified no local probes).
SUT: EC2 c6id.2xlarge (8 vCPU), XFS on local NVMe, `multithreaded=on`, pooled protocol mode.

--------------------------------------------------------------------------------
## Summary

Under the **default** threaded mode (pooled protocol scheduler), a client session that has a query
pending **never releases its carrier**. With more concurrent busy sessions than carriers, the
excess sessions make **essentially zero progress for the entire run** — they are not slowed, they
are starved. Aggregate tps looks *fine* (even better than fork), which is exactly why this hid:
the working set of sessions saturates the box while the rest sit idle.

This came out of reviewing the new P1 harness's own validation data. Credit where due: the harness
found it because requirement 9 makes it record per-run max latency from the per-transaction log,
and requirement 7 forced a separate loadgen so the numbers were clean.

## The law, measured

`pgbench -S` (read-only), 20 s, `--log`, counting per-client completed transactions.
A client is "starved" if it completed <= 2 transactions in 20 s (healthy clients do ~10^5).

| lane | clients | carriers_eff | working | **starved** | tps |
|---|---|---|---|---|---|
| xtc | 4 | 8 | 4 | 0 | 50,757 |
| xtc | 8 | 8 | 8 | 0 | 103,321 |
| xtc | 12 | 8 | 8 | **4** | 103,028 |
| xtc | 16 | 8 | 8 | **8** | 103,251 |
| xtc | 24 | 8 | 8 | **16** | 102,706 |
| xtc | 32 | 8 | 8 | **24** | 103,297 |
| xtc | 16 | **4** | 4 | **12** | 50,792 |
| fork | 16 | — | 16 | **0** | 94,477 |
| fork | 32 | — | 32 | **0** | 89,100 |

**`working = min(clients, carriers_eff)`, exactly, every time.** The last xtc row is the control
that matters: dropping carriers 8 -> 4 with clients fixed at 16 moves working 8 -> 4. It tracks
**carriers**, not cores, not clients. Fork starves nobody.

Per-client detail from one c=16 / 8-carrier run — the shape is unmistakable:

```
client  0: 379005 txns      client  1: 1 txn
client  8: 378823 txns      client  2: 1 txn
client  9: 379080 txns      client  3: 1 txn
client 10: 378550 txns      client  4: 1 txn
client 11: 378736 txns      client  5: 1 txn
client 12: 378940 txns      client  6: 1 txn
client 13: 379035 txns      client  7: 1 txn
client 14: 378520 txns      client 15: 1 txn
```

Eight clients did **one** transaction each — their first — and that transaction's recorded latency
is **29,978 ms of a 30,000 ms run** (i.e. it was issued at t=0 and answered at the very end, when
the load stopped and carriers freed up). Reproduced **3/3** runs with near-identical values
(29978.157 / 29978.281 / 29978.402 ms). Zero connection errors in the server log.

## Root cause: the carrier is only released at a protocol read park

`PgSessionRunProtocolSchedulerUntilBoundary()` (`src/backend/tcop/postgres.c:7068`) says so in its
own comment:

> *"Returning to the carrier loop is still controlled only by protocol parks or logical exit."*

Its loop returns to the carrier on exactly three outcomes:
* `PG_STEP_PARK_PROTOCOL_READ` — the session is waiting for the **next client message**;
* `PG_STEP_DONE` / `PG_STEP_FATAL_EXIT` — the session is over.

`PG_STEP_CONTINUE` just loops. So a session yields its carrier only when it has **nothing left to
do**. A pgbench client in a tight loop always has the next query in flight, so it never reaches a
protocol read park and **holds its carrier for the whole run**. The queued sessions are not
descheduled-and-resumed; they are simply never attached.

### Two falsification tests I ran, and what they showed

1. **Is it a pgbench artifact (client threads)?** Same 16 clients, sweeping `-j` 1/4/8/16:
   **8 working / 8 starved at every value.** Invariant to `-j` ⇒ not a client-side artifact.
2. **Does multiplexing work at all?** 12 plain `psql` clients against **4** carriers:
   **all 12 completed.** This **refutes the strong form** of the hypothesis ("only N sessions can
   ever be served"). Short sessions that finish, or that park waiting for their next command,
   hand the carrier on correctly. The bug needs sessions that stay *continuously busy*.

That second result is why this is a fairness/preemption bug and not a "pool is broken" bug, and
I would have reported the wrong thing had I stopped at test 1.

## Why this matters more than the numbers suggest

* **It is the default mode.** `pooled_protocol_carriers = -1` (auto) is the default under
  `multithreaded=on`; auto resolves to core count. So any deployment with more busy connections
  than cores starves the excess — silently.
* **Aggregate tps hides it completely.** 103k tps at c=32 looks like clean scaling; in fact 8
  sessions get everything and 24 get nothing. Mean latency looks fine too. **Only per-client
  fairness or max latency reveals it** — p99 was 0.097 ms in the very same run whose max was 30 s.
* **This is very likely the real content of the "flat scaling" I reported an hour ago**
  (xtc flat 54k -> 54k from c=8 -> c=16 while fork went 54k -> 96k). It is not that xtc fails to
  scale; it is that xtc *stops adding sessions* past the carrier count. Same root cause,
  and it supersedes my "concurrency ceiling" framing in
  `.ec2/p1-harness-2026-09-10/README.md`.
* It is **independent of both open hangs**: read-only (no WAL/commit/fsync, so not the
  `xtc_aio_fdatasync` lost-wake) and it completes rather than wedging (so not the
  checkpointer/fiber buffer-lock deadlock in `THREADED_TEST_BASELINE_2026-09.md`).

## What is NOT established

* Whether a **wait boundary other than protocol-read** (lock wait, I/O wait, CV wait) does release
  the carrier. The scheduler is documented as wait-boundary aware (Phase 13/14), and this test only
  proves the *protocol-read* boundary is the one governing attachment. A session blocking on a lock
  may well yield correctly. Untested.
* Whether this is **intentional for now**. Phase 15 notes describe pooled mode as multiplexing
  "sessions over carriers", and `009_phase15_pooled_deep_waits_pinned.pl` exists, which suggests
  deep-wait yielding was in scope. But I have not read the Phase 15 design closely enough to say
  whether unbounded hold-until-idle is a known limitation or a regression.
* Any claim about **fairness under a mixed workload** (some idle, some busy). Only measured
  all-busy.

## Suggested fix direction (design, not implemented)

The scheduler needs a **preemption point that does not depend on the client going quiet**:
* release the carrier at the **end of each completed statement/transaction** when other sessions are
  queued (cheapest, and the natural analogue of a protocol boundary), and/or
* an explicit **time-slice / round-robin** over attached sessions, and/or
* treat "runnable sessions > carriers" as a **demand signal to grow the pool** (there is already a
  demand-grow path — `backend_pooled_protocol_maybe_grow_for_runnable_demand` — worth checking why
  it does not fire here).

Any of these is a hot-path scheduler change, so per the branch rules it needs an EC2 A/B that is
neutral-or-better on read-`-S` and CPU-bound, plus two independent reviews.

## Repro (about 3 minutes)

```
postgres -c multithreaded=on -c pooled_protocol_carriers=4 -c max_connections=200 ...
pgbench -i -s 20
pgbench -n -S -T 20 -c 16 -j 16 --log
awk '{c[$1]++} END{for(k in c) print k, c[k]}' pgbench_log.* | sort -k2 -n
# => 4 clients with ~10^5 txns, 12 clients with exactly 1
```

Artifacts: `.ec2/starve-2026-09-10/` (fairness sweep, `-j` sweep, per-txn logs, psql control).
